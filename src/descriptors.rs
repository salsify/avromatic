use avro_rs::{
    FullSchema,
    types::ToAvro,
    schema::{SchemaKind, SchemaFingerprint, SchemaIter, SchemaRef, UnionRef},
    types::Value as AvroValue,
};
use crate::heap_guard::HeapGuard;
use crate::model::{AvromaticModel, ModelStorage, MODEL_STORAGE_WRAPPER};
use crate::serializer;
use crate::util::{instance_of, RDate, RDateTime, RTime};
use crate::values::AvromaticValue;
use failure::{Error, Fail, format_err};
use rutie::*;
use sha2::Sha256;
use std::collections::HashMap;

#[derive(Debug, Fail)]
pub enum AvromaticError {
    #[fail(display = "attribute '{}' does not exist", name)]
    InvalidAttribute {
        name: String,
    },
    #[fail(display = "cannot coerce {} to {}", value, name)]
    InvalidValue {
        value: String,
        name: String,
    },
}

struct ModelRecord {
    schema: FullSchema,
    descriptor: RecordDescriptor,
}

impl ModelRecord {
    fn new(schema: FullSchema) -> Result<Self, Error> {
        let descriptor = RecordDescriptor::build(&schema)?;
        Ok(Self { schema, descriptor })
    }
}

pub struct ModelDescriptorInner {
    key: Option<ModelRecord>,
    value: ModelRecord,
}

wrappable_struct!(
    ModelDescriptorInner,
    ModelDescriptorWrapper,
    MODEL_DESCRIPTOR_WRAPPER,
    mark(value) {
        if let Some(ref key) = value.key {
            key.descriptor.mark();
        }
        value.value.descriptor.mark();
    }
);

impl ModelDescriptorInner {
    pub fn new(key_schema: Option<FullSchema>, value_schema: FullSchema) -> Result<Self, Error> {
        let key = key_schema.map_or(Ok(None), |k| ModelRecord::new(k).map(Some))?;
        let value = ModelRecord::new(value_schema)?;
        if let Some(ref key) = key {
            let values = &value.descriptor.attributes;
            key.descriptor.attributes.iter().for_each(|(k, key_type)| {
                if let Some(value_type) = values.get(k) {
                    if value_type != key_type {
                        let message = format!(
                            "Field '{}' has a different type in each schema: {:?} {:?}",
                            k,
                            key_type,
                            value_type,
                        );
                        VM::raise(Class::from_existing("RuntimeError"), &message);
                    }
                }
            });
        }
        Ok(Self { key, value })
    }

    pub fn coerce(&self, key: &str, value: AnyObject, guard: &mut HeapGuard) -> Result<AvromaticValue, Error> {
        self.value.descriptor.coerce(key, value, guard)
    }

    pub fn to_ruby(&self, key: &str, value: &AvromaticValue) -> AnyObject {
        self.value.descriptor.to_ruby(key, value)
    }

    pub fn serialize(&self, attributes: &HashMap<String, AvromaticValue>)
        -> Result<Vec<u8>, Error>
    {
        serializer::serialize(&self.value.schema, attributes)
    }

    pub fn deserialize(&self, class: &Class, data: &[u8], guard: &mut HeapGuard) -> Result<AnyObject, Error> {
        let mut cursor = std::io::Cursor::new(data);
        // TODO: need to get writer schema
        let value = avro_rs::from_avro_datum(&self.value.schema, &mut cursor, None)?;
        self.avro_to_model(class, &value, guard)
    }

    fn avro_to_model(&self, class: &Class, value: &AvroValue, guard: &mut HeapGuard)
        -> Result<AnyObject, Error>
    {
        let attributes = self.value.descriptor.avro_to_attributes(value, guard)?;
        let storage = ModelStorage { attributes };
        let mut model = class.allocate().to_any_object();
        guard.guard(model.to_any_object());
        let wrapped_storage: AnyObject = class.wrap_data(storage, &*MODEL_STORAGE_WRAPPER);
        model.instance_variable_set("@_attributes", wrapped_storage);
        Ok(model)
    }

    pub fn each_field<F>(&self, mut f: F)
        where F: FnMut(&str, &AttributeDescriptor)
    {
        if let Some(ref key) = self.key {
            key.descriptor.attributes.iter().for_each(|(k, v)| f(k, v));
        }
        self.value.descriptor.attributes.iter().for_each(|(k, v)| f(k, v));
    }

    pub fn get_attribute(&self, name: &str) -> Option<&AttributeDescriptor> {
        self.value.descriptor.attributes.get(name)
    }

    pub fn fingerprint(&self) -> Vec<u8> {
        println!("{:?}", self.value.schema.schema);
        self.value.schema.schema.fingerprint::<Sha256>().bytes
    }

    pub fn value_schema(&self) -> &FullSchema {
        &self.value.schema
    }
}

class!(ModelDescriptor);

impl ModelDescriptor {
    pub fn new(key_schema: Option<FullSchema>, value_schema: FullSchema) -> Result<Self, Error> {
        let inner = ModelDescriptorInner::new(key_schema, value_schema)?;
        let this = Class::from_existing("ModelDescriptor")
            .wrap_data(inner, &*MODEL_DESCRIPTOR_WRAPPER);
        Ok(this)
    }
}

#[derive(Debug)]
struct RecordDescriptor {
    attributes: HashMap<String, AttributeDescriptor>,
}

impl RecordDescriptor {
    pub fn build(schema: &FullSchema) -> Result<Self, Error> {
        let record = schema.record_schema()
            .ok_or_else(|| format_err!("Invalid Schema"))?;
        let attributes = record.fields().iter().map(|field| {
            let attribute = AttributeDescriptor::build(
                field.schema(),
                field.default().map(|v| v.clone().avro()),
            )?;
            Ok((field.name().to_string(), attribute))
        }).collect::<Result<HashMap<String, AttributeDescriptor>, Error>>()?;
        Ok(Self { attributes })
    }

    pub fn coerce(&self, key: &str, value: AnyObject, guard: &mut HeapGuard) -> Result<AvromaticValue, Error> {
        self.attributes.get(key)
            .ok_or_else(|| AvromaticError::InvalidAttribute{ name: key.to_string() }.into())
            .and_then(|descriptor| descriptor.coerce(value, guard))
    }

    pub fn to_ruby(&self, key: &str, value: &AvromaticValue) -> AnyObject {
        self.attributes.get(key)
            .map(|descriptor| descriptor.to_ruby(value))
            .unwrap_or_else(|| NilClass::new().into())
    }

    fn mark(&self) {
        self.attributes.values().for_each(AttributeDescriptor::mark);
    }

    fn avro_to_attributes(&self, value: &AvroValue, guard: &mut HeapGuard)
        -> Result<HashMap<String, AvromaticValue>, Error>
    {
        match value {
            AvroValue::Record(fields) => {
                let mut attributes = HashMap::new();
                fields.into_iter().map(|(key, value)| {
                    let descriptor = self.attributes.get(key)
                        .unwrap();
                    let attribute = descriptor.avro_to_attribute(value, guard)?;
                    attributes.insert(key.to_string(), attribute);
                    Ok(())
                }).collect::<Result<Vec<()>, Error>>()?;
                Ok(attributes)
            },
            _ => unimplemented!(),
        }
    }
}

#[derive(Debug, PartialEq)]
pub struct AttributeDescriptor {
    type_descriptor: TypeDescriptor,
    default: Option<AvromaticValue>,
}

impl AttributeDescriptor {
    pub fn build<'a>(field_schema: SchemaRef<'a>, default: Option<AvroValue>) -> Result<Self, Error> {
        let type_descriptor = TypeDescriptor::build(field_schema)?;
        let default = default.map(|v| {
            type_descriptor.avro_to_attribute(&v, &mut HeapGuard::new())
        });
        let default = match default {
            Some(Ok(default)) => Some(default),
            Some(Err(err)) => return Err(err),
            None => None,
        };
        Ok(Self { type_descriptor, default })
    }

    pub fn coerce(&self, value: AnyObject, guard: &mut HeapGuard) -> Result<AvromaticValue, Error> {
        self.type_descriptor.coerce(&value, guard)
    }

    pub fn to_ruby(&self, value: &AvromaticValue) -> AnyObject {
        self.type_descriptor.to_ruby(&value)
    }

    pub fn default(&self) -> AnyObject {
        self.default
            .as_ref()
            .map(|v| self.to_ruby(v))
            .unwrap_or_else(|| NilClass::new().into())
    }

    fn mark(&self) {
        self.type_descriptor.mark();
        if let Some(v) = &self.default {
            v.mark();
        }
    }

    fn avro_to_attribute(&self, value: &AvroValue, guard: &mut HeapGuard)
        -> Result<AvromaticValue, Error>
    {
        self.type_descriptor.avro_to_attribute(value, guard)
    }

    pub fn is_boolean(&self) -> bool {
        if let TypeDescriptor::Boolean = self.type_descriptor {
            true
        } else if let TypeDescriptor::Union(_, ref variants) = self.type_descriptor{
            variants.len() == 2
                && variants[0] == TypeDescriptor::Null
                && variants[1] == TypeDescriptor::Boolean
        } else {
            false
        }
    }
}

#[derive(Debug, PartialEq)]
enum TypeDescriptor  {
    Boolean,
    Enum(Vec<String>),
    Fixed(usize),
    Float,
    Integer,
    Null,
    String,
    Bytes,

    Date,
    TimestampMicros,
    TimestampMillis,

    Array(Box<TypeDescriptor>),
    Map(Box<TypeDescriptor>),
    Record(Class),
    Union(HashMap<UnionRef, usize>, Vec<TypeDescriptor>),
}

impl TypeDescriptor {
    pub fn build(schema: SchemaRef) -> Result<Self, Error> {
        let out = match schema.kind() {
            SchemaKind::Null => TypeDescriptor::Null,
            SchemaKind::Boolean => TypeDescriptor::Boolean,
            SchemaKind::Int => TypeDescriptor::Integer,
            SchemaKind::Long => TypeDescriptor::Integer,
            SchemaKind::Float => TypeDescriptor::Float,
            SchemaKind::Double => TypeDescriptor::Float,
            SchemaKind::Bytes => TypeDescriptor::Bytes,
            SchemaKind::String => TypeDescriptor::String,
            SchemaKind::Date => TypeDescriptor::Date,
            SchemaKind::TimestampMicros => TypeDescriptor::TimestampMicros,
            SchemaKind::TimestampMillis => TypeDescriptor::TimestampMillis,
            SchemaKind::TimeMillis => unimplemented!(),
            SchemaKind::TimeMicros => unimplemented!(),
            SchemaKind::Fixed => {
                let size = schema.fixed_size()
                    .ok_or_else(|| format_err!("Invalid Schema"))?;
                TypeDescriptor::Fixed(size)
            },
            SchemaKind::Array => {
                let schema = schema.array_schema()
                    .ok_or_else(|| format_err!("Invalid Schema"))?;
                let inner = TypeDescriptor::build(schema)?;
                TypeDescriptor::Array(Box::new(inner))
            },
            SchemaKind::Map => {
                let schema = schema.map_schema()
                    .ok_or_else(|| format_err!("Invalid Schema"))?;
                let inner = TypeDescriptor::build(schema)?;
                TypeDescriptor::Map(Box::new(inner))
            },
            SchemaKind::Union => {
                let union_schema = schema.union_schema()
                    .ok_or_else(|| format_err!("Invalid Schema"))?;
                let variants = union_schema
                    .variants()
                    .into_iter()
                    .map(TypeDescriptor::build)
                    .collect::<Result<Vec<TypeDescriptor>, Error>>()?;
                TypeDescriptor::Union(union_schema.union_ref_map(), variants)
            },
            SchemaKind::Record => {
                let inner = AvromaticModel::build_model(schema.as_full_schema());
                TypeDescriptor::Record(inner)
            },
            SchemaKind::Enum => {
                let symbols = schema.enum_symbols()
                    .ok_or_else(|| format_err!("Invalid Schema"))?;
                TypeDescriptor::Enum(symbols.to_vec())
            },
        };
        Ok(out)
    }

    pub fn coerce(&self, value: &AnyObject, guard: &mut HeapGuard) -> Result<AvromaticValue, Error> {
        if value.is_nil() {
            return Ok(AvromaticValue::Null);
        }
        match self {
            TypeDescriptor::Null => coerce_null(value),
            TypeDescriptor::Boolean => coerce_boolean(value),
            TypeDescriptor::String => coerce_string(value, guard),
            TypeDescriptor::Bytes => coerce_string(value, guard),
            TypeDescriptor::Enum(symbols) => coerce_enum(value, symbols, guard),
            TypeDescriptor::Integer => coerce_integer(value, guard),
            TypeDescriptor::Date => coerce_date(value, guard),
            TypeDescriptor::TimestampMillis => coerce_timestamp_millis(value, guard),
            TypeDescriptor::TimestampMicros => coerce_timestamp_micros(value, guard),
            TypeDescriptor::Float => coerce_float(value, guard),
            TypeDescriptor::Fixed(length) => coerce_fixed(value, *length, guard),
            TypeDescriptor::Array(inner) => coerce_array(value, inner, guard),
            TypeDescriptor::Union(_, variants) => coerce_union(value, variants, guard),
            TypeDescriptor::Record(inner) => coerce_record(value, inner, guard),
            TypeDescriptor::Map(inner) => coerce_map(value, inner, guard),
        }
    }

    pub fn to_ruby(&self, value: &AvromaticValue) -> AnyObject {
        match (value, self) {
            (AvromaticValue::Null, _) => NilClass::new().into(),
            (AvromaticValue::True, _) => Boolean::new(true).to_any_object(),
            (AvromaticValue::False, _) => Boolean::new(false).to_any_object(),
            (AvromaticValue::String(string), _) => string.to_any_object(),
            (AvromaticValue::Long(n), TypeDescriptor::Date) => RDate::from_i64(n),
            (AvromaticValue::Long(n), TypeDescriptor::TimestampMillis) => RTime::from_millis(n),
            (AvromaticValue::Long(n), TypeDescriptor::TimestampMicros) => RTime::from_micros(n),
            (AvromaticValue::Long(n), _) => n.to_any_object(),
            (AvromaticValue::Float(f), _) => f.to_any_object(),
            (AvromaticValue::Array(values), TypeDescriptor::Array(inner)) => {
                values.iter().map(|v| inner.to_ruby(v)).collect::<Array>().to_any_object()
            },
            (AvromaticValue::Union(index, value), TypeDescriptor::Union(_, variants)) => {
                variants[*index].to_ruby(value)
            },
            (AvromaticValue::Record(value), _) => value.to_any_object(),
            (AvromaticValue::Map(value), TypeDescriptor::Map(inner)) => {
                let mut hash = Hash::new();
                value.iter().for_each(|(k, v)| {
                    hash.store(RString::new_utf8(k), inner.to_ruby(v));
                });
                hash.to_any_object()
            },
            _ => unreachable!(),
        }
    }

    fn mark(&self) {
        match self {
            TypeDescriptor::Array(inner) => inner.mark(),
            TypeDescriptor::Union(_, variants) => variants.iter().for_each(TypeDescriptor::mark),
            TypeDescriptor::Record(inner) => GC::mark(inner),
            _ => (),
        }
    }

    fn avro_to_attribute(&self, value: &AvroValue, guard: &mut HeapGuard)
        -> Result<AvromaticValue, Error>
    {
        let out = match (self, value) {
            (TypeDescriptor::Null, _) => AvromaticValue::Null,
            (TypeDescriptor::Boolean, AvroValue::Boolean(true)) =>
                AvromaticValue::True,
            (TypeDescriptor::Boolean, AvroValue::Boolean(false)) =>
                AvromaticValue::True,
            (TypeDescriptor::Bytes, AvroValue::Bytes(bytes)) |
            (TypeDescriptor::Fixed(_), AvroValue::Fixed(_, bytes)) => {
                let encoding = Encoding::find("ASCII-8BIT").unwrap();
                let rstring = RString::from_bytes(&bytes, &encoding);
                guard.guard(rstring.to_any_object());
                AvromaticValue::String(rstring)
            },
            (TypeDescriptor::Enum(_), AvroValue::Enum(_, s)) |
            (TypeDescriptor::Enum(_), AvroValue::String(s)) |
            (TypeDescriptor::String, AvroValue::String(s)) => {
                let rstring: RString = RString::new_utf8(s);
                guard.guard(rstring.to_any_object());
                AvromaticValue::String(rstring)
            },
            (TypeDescriptor::Integer, AvroValue::Int(n)) |
            (TypeDescriptor::Date, AvroValue::Date(n)) => {
                AvromaticValue::Long((*n as i64).into())
            },
            (TypeDescriptor::Integer, AvroValue::Long(n)) |
            (TypeDescriptor::TimestampMicros, AvroValue::TimestampMicros(n)) |
            (TypeDescriptor::TimestampMillis, AvroValue::TimestampMillis(n)) => {
                AvromaticValue::Long((*n).into())
            },
            (TypeDescriptor::Float, AvroValue::Float(n)) => {
                let f = Float::new(*n as f64);
                guard.guard(f.to_any_object());
                AvromaticValue::Float(f)
            },
            (TypeDescriptor::Float, AvroValue::Double(n)) => {
                let f = Float::new(*n);
                guard.guard(f.to_any_object());
                AvromaticValue::Float(f)
            },
            (TypeDescriptor::Map(inner), AvroValue::Map(values)) => {
                let map = values.into_iter()
                    .map(|(k, v)| Ok((k.to_string(), inner.avro_to_attribute(v, guard)?)))
                    .collect::<Result<HashMap<String, AvromaticValue>, Error>>()?;
                AvromaticValue::Map(map)
            },
            (TypeDescriptor::Array(inner), AvroValue::Array(values)) => {
                let attributes = values.into_iter()
                    .map(|v| inner.avro_to_attribute(v, guard))
                    .collect::<Result<Vec<AvromaticValue>, Error>>()?;
                AvromaticValue::Array(attributes)
            },
            (TypeDescriptor::Union(ref_index, schemas), AvroValue::Union(union_ref, value)) => {
                if let Some(index) = ref_index.get(&union_ref) {
                    let value = schemas[*index].avro_to_attribute(value, guard)?;
                    AvromaticValue::Union(*index, Box::new(value))
                } else {
                    unimplemented!()
                }
            },
            (TypeDescriptor::Union(_, schemas), value) => {
                for schema in schemas {
                    if let Ok(value) = schema.avro_to_attribute(value, guard) {
                        return Ok(value);
                    }
                }
                return Err(format_err!("Failed to convert avro '{:?}' to {:?}", value, self));
            },
            (TypeDescriptor::Record(inner), value) => {
                let schema = inner.send("_schema", None);
                let descriptor = schema.get_data(&*MODEL_DESCRIPTOR_WRAPPER);
                let record = descriptor.avro_to_model(inner, value, guard)?;
                guard.guard(record.to_any_object());
                AvromaticValue::Record(record)
            },
            _ => return Err(format_err!("Failed to convert avro '{:?}' to {:?}", value, self)),
        };
        Ok(out)
    }
}

fn bad_coercion(value: &AnyObject, name: &str) -> Error {
    AvromaticError::InvalidValue {
        value: value.send("inspect", None).try_convert_to::<RString>().unwrap().to_string(),
        name: name.to_string(),
    }.into()
}

fn coerce_null(value: &AnyObject) -> Result<AvromaticValue, Error> {
    if value.is_nil() {
        Ok(AvromaticValue::Null)
    } else {
        Err(bad_coercion(value, "boolean"))
    }
}

fn coerce_boolean(value: &AnyObject) -> Result<AvromaticValue, Error> {
    value.try_convert_to::<Boolean>()
        .map(|b| if b.to_bool() { AvromaticValue::True } else { AvromaticValue::False })
        .or_else(|_| Err(bad_coercion(value, "null")))
}

fn convert_string(value: &AnyObject) -> Result<RString, AnyException> {
    value.try_convert_to::<RString>()
        .or_else(|_| {
            value.try_convert_to::<Symbol>()
                .and_then(|symbol| symbol.send("to_s", None).try_convert_to())
        })
}

fn coerce_string(value: &AnyObject, guard: &mut HeapGuard) -> Result<AvromaticValue, Error> {
    convert_string(value)
        .map(|string| {
            guard.guard(string.to_any_object());
            string
        })
        .map(AvromaticValue::String)
        .or_else(|_| Err(bad_coercion(value, "string")))
}

fn coerce_fixed(value: &AnyObject, length: usize, guard: &mut HeapGuard) -> Result<AvromaticValue, Error> {
    value.try_convert_to::<RString>()
        .ok()
        .and_then(|rstring| if rstring.to_str().len() != length {
            None
        } else {
            guard.guard(rstring.to_any_object());
            Some(AvromaticValue::String(rstring))
        })
        .ok_or_else(|| bad_coercion(value, &format!("fixed({})", length)))
}

fn coerce_date(value: &AnyObject, guard: &mut HeapGuard) -> Result<AvromaticValue, Error> {
    let n = if let Ok(date) = value.try_convert_to::<RDate>() {
        date.days_since_epoch()
    } else if let Ok(datetime) = value.try_convert_to::<RDateTime>() {
        datetime.days_since_epoch()
    } else if let Ok(time) = value.try_convert_to::<RTime>() {
        time.days_since_epoch()
    } else {
        return Err(bad_coercion(value, "date"))
    };
    guard.guard(n.to_any_object());
    Ok(AvromaticValue::Long(n))
}

fn coerce_timestamp_millis(value: &AnyObject, guard: &mut HeapGuard) -> Result<AvromaticValue, Error> {
    if instance_of(value, Class::from_existing("Numeric")) {
        let n = value.send("to_i", None).try_convert_to::<Integer>()
            .map_err(|_| bad_coercion(value, "timestamp-millis"))?;
        guard.guard(n.to_any_object());
        return Ok(AvromaticValue::Long(n));
    }

    if value.class() == Class::from_existing("Date") {
        return Err(bad_coercion(value, "timestamp-millis"));
    }

    let n = value.send("to_time", None)
        .try_convert_to::<RTime>()
        .map(|time| time.to_millis())
        .map_err(|_| bad_coercion(value, "timestamp-millis"))?;
    guard.guard(n.to_any_object());
    Ok(AvromaticValue::Long(n))
}

fn coerce_timestamp_micros(value: &AnyObject, guard: &mut HeapGuard) -> Result<AvromaticValue, Error> {
    if instance_of(value, Class::from_existing("Numeric")) {
        let n = value.send("to_i", None).try_convert_to::<Integer>()
            .map_err(|_| bad_coercion(value, "timestamp-micros"))?;
        guard.guard(n.to_any_object());
        return Ok(AvromaticValue::Long(n));
    }

    if value.class() == Class::from_existing("Date") {
        return Err(bad_coercion(value, "timestamp-micros"));
    }

    let n = value.send("to_time", None)
        .try_convert_to::<RTime>()
        .map(|time| time.to_micros())
        .map_err(|_| bad_coercion(value, "timestamp-micros"))?;
    guard.guard(n.to_any_object());
    Ok(AvromaticValue::Long(n))
}

fn coerce_integer(value: &AnyObject, guard: &mut HeapGuard) -> Result<AvromaticValue, Error> {
    value.try_convert_to::<Integer>()
        .map(|int| {
            guard.guard(int.to_any_object());
            int
        })
        .map(AvromaticValue::Long)
        .or_else(|_| Err(bad_coercion(value, "integer")))
}

fn coerce_float(value: &AnyObject, guard: &mut HeapGuard) -> Result<AvromaticValue, Error> {
    value.try_convert_to::<Float>()
        .or_else(|_|
            value.try_convert_to::<Integer>()
                .map(|i| Float::new(i.to_i64() as f64))
        )
        .map(|float| {
            guard.guard(float.to_any_object());
            float
        })
        .map(AvromaticValue::Float)
        .map_err(|_| bad_coercion(value, "float"))
}

fn coerce_enum(value: &AnyObject, symbols: &[String], guard: &mut HeapGuard) -> Result<AvromaticValue, Error> {
    value.try_convert_to::<RString>()
        .or_else(|_| {
            value.try_convert_to::<Symbol>()
                .and_then(|symbol| symbol.send("to_s", None).try_convert_to())
        })
        .ok()
        .and_then(|string| {
            let sym = string.to_str();
            symbols.iter().find(|v| v.as_str() == sym).map(|_| string)
        })
        .map(|string| {
            guard.guard(string.to_any_object());
            string
        })
        .map(AvromaticValue::String)
        .ok_or_else(|| bad_coercion(value, "string"))
}

fn coerce_array(value: &AnyObject, inner: &TypeDescriptor, guard: &mut HeapGuard)
    -> Result<AvromaticValue, Error>
{
    value.try_convert_to::<Array>()
        .map_err(|_| bad_coercion(value, "array"))
        .and_then(|array| array.into_iter().map(|element| inner.coerce(&element, guard)).collect())
        .map(AvromaticValue::Array)
}

fn coerce_union(value: &AnyObject, variants: &[TypeDescriptor], guard: &mut HeapGuard)
    -> Result<AvromaticValue, Error>
{
    variants.iter()
        .enumerate()
        .map(|(i, variant)| Ok(AvromaticValue::Union(i, Box::new(variant.coerce(&value, guard)?))))
        .find(Result::is_ok)
        .unwrap_or_else(|| Err(bad_coercion(value, "union")))
}

fn coerce_map(value: &AnyObject, inner: &TypeDescriptor, guard: &mut HeapGuard)
    -> Result<AvromaticValue, Error>
{
    let hash = value.try_convert_to::<Hash>().map_err(|_| bad_coercion(value, "map"))?;
    let mut error = Ok(());
    let mut map = HashMap::new();
    hash.each(|key, value| {
        if error.is_err() {
            return;
        }

        let maybe_k = convert_string(&key);
        if let Err(_) = maybe_k {
            error = Err(bad_coercion(&key, "string"));
            return;
        }
        let maybe_v = inner.coerce(&value, guard);
        if let Err(err) = maybe_v {
            error = Err(err);
            return;
        }
        map.insert(maybe_k.unwrap().to_string(), maybe_v.unwrap());
    });
    error.map(|_| AvromaticValue::Map(map))
}

fn coerce_record(value: &AnyObject, record_class: &Class, guard: &mut HeapGuard)
    -> Result<AvromaticValue, Error>
{
    let valid_object = value.class().ancestors().iter().any(|class| class.is_eql(record_class));
    if valid_object {
        guard.guard(value.to_any_object());
        return Ok(AvromaticValue::Record(value.to_any_object()))
    }
    let record = record_class.send("new", Some(&[value.to_any_object()]));
    guard.guard(record.to_any_object());
    Ok(AvromaticValue::Record(record))
}

pub fn initialize() {
    Class::new("ModelDescriptor", None).define(|_| ());
}

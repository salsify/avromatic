use avro_rs::{
    FullSchema,
    types::ToAvro,
    schema::{SchemaKind, SchemaIter, SchemaRef, UnionRef},
    types::Value as AvroValue,
};
use crate::custom_types::{CustomTypeConfiguration, CustomTypeRegistry};
use crate::de::*;
use crate::heap_guard::HeapGuard;
use crate::model::{AvromaticModel, ModelStorage, MODEL_STORAGE_WRAPPER};
use crate::model_pool::ModelRegistry;
use crate::util::{instance_of, RDate, RDateTime, RTime};
use crate::values::AvromaticValue;
use failure::{Error, Fail, format_err};
use rutie::*;
use std::collections::HashMap;
use std::io::Read;
use std::mem::transmute;

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
    fn new(schema: FullSchema, nested_models: &ModelRegistry) -> Result<Self, Error> {
        let descriptor = RecordDescriptor::build(&schema, nested_models)?;
        Ok(Self { schema, descriptor })
    }

    fn serialize(&self, values: &HashMap<String, AvromaticValue>, buf: &mut Vec<u8>)
                 -> Result<(), Error>
    {
        let datum = self.descriptor.serialize(values, &self.schema)?;
        avro_rs::write_avro_datum(&self.schema, datum, buf)
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
    pub fn new(
        key_schema: Option<FullSchema>,
        value_schema: FullSchema,
        nested_models: &ModelRegistry,
    ) -> Result<Self, Error> {
        let key = key_schema.map_or(Ok(None), |k| ModelRecord::new(k, nested_models).map(Some))?;
        let value = ModelRecord::new(value_schema, nested_models)?;
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
        self.value.descriptor.coerce(key, value.to_any_object(), guard).or_else(|err| {
            self.key.as_ref().map(|k| k.descriptor.coerce(key, value, guard))
                .unwrap_or(Err(err))
        })
    }

    pub fn to_ruby(&self, key: &str, value: &AvromaticValue) -> AnyObject {
        self.key.as_ref()
            .and_then(|k| k.descriptor.to_ruby(key, value))
            .or_else(|| self.value.descriptor.to_ruby(key, value))
            .unwrap_or_else(|| NilClass::new().into())
    }

    pub fn serialize_key(
        &self,
        attributes: &HashMap<String, AvromaticValue>,
        buf: &mut Vec<u8>,
    ) -> Result<(), Error>
    {
        self.key
            .as_ref()
            .map(|key| key.serialize(attributes, buf))
            .ok_or_else(|| format_err!("Model has no key schema"))
            .and_then(|x| x)
    }

    pub fn serialize_value(
        &self,
        attributes: &HashMap<String, AvromaticValue>,
        buf: &mut Vec<u8>,
    ) -> Result<(), Error>
    {
        self.value.serialize(attributes, buf)
    }

    pub fn deserialize_message(
        &self,
        class: &Class,
        key: &[u8],
        value: &[u8],
        guard: &mut HeapGuard,
    ) -> Result<AnyObject, Error> {
        let mut key_cursor = std::io::Cursor::new(key);
        let mut value_cursor = std::io::Cursor::new(value);
        // TODO: need to get writer schema
        let key = avro_rs::from_avro_datum(&self.key.as_ref().unwrap().schema, &mut key_cursor, None)?;
        let value = avro_rs::from_avro_datum(&self.value.schema, &mut value_cursor, None)?;
        self.avro_to_message_model(class, &key, &value, guard)
    }

    pub fn deserialize_value(
        &self,
        class: &Class,
        data: &[u8],
        writer_schema: &FullSchema,
        guard: &mut HeapGuard,
    ) -> Result<AnyObject, Error> {
        let mut cursor = std::io::Cursor::new(data);
        // TODO: need to get writer schema
        let value = avro_rs::from_avro_datum(&self.value.schema, &mut cursor, Some(writer_schema))?;
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

    fn avro_to_message_model(
        &self,
        class: &Class,
        key: &AvroValue,
        value: &AvroValue,
        guard: &mut HeapGuard,
    ) -> Result<AnyObject, Error>
    {
        let key_attributes = self.key.as_ref().unwrap().descriptor.avro_to_attributes(key, guard)?;
        let mut attributes = self.value.descriptor.avro_to_attributes(value, guard)?;
        attributes.extend(key_attributes);
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

    pub fn each_key_field<F>(&self, mut f: F)
        where F: FnMut(&str, &AttributeDescriptor)
    {
        if let Some(ref key) = self.key {
            key.descriptor.attributes.iter().for_each(|(k, v)| f(k, v));
        }
    }

    pub fn each_value_field<F>(&self, mut f: F)
        where F: FnMut(&str, &AttributeDescriptor)
    {
        self.value.descriptor.attributes.iter().for_each(|(k, v)| f(k, v));
    }

    pub fn get_attribute(&self, name: &str) -> Option<&AttributeDescriptor> {
        self.value.descriptor.attributes.get(name)
    }

    pub fn value_schema(&self) -> &FullSchema {
        &self.value.schema
    }
}

class!(ModelDescriptor);

impl ModelDescriptor {
    pub fn new(
        key_schema: Option<FullSchema>,
        value_schema: FullSchema,
        nested_models: &ModelRegistry,
    ) -> Result<Self, Error> {
        let inner = ModelDescriptorInner::new(key_schema, value_schema, nested_models)?;
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
    pub fn build(schema: &FullSchema, nested_models: &ModelRegistry) -> Result<Self, Error> {
        let record = schema.record_schema()
            .ok_or_else(|| format_err!("Invalid Schema"))?;
        let attributes = record.fields().iter().map(|field| {
            let attribute = AttributeDescriptor::build(
                field.schema(),
                field.default().map(|v| v.clone().avro()),
                nested_models,
            )?;
            Ok((field.name().to_string(), attribute))
        }).collect::<Result<HashMap<String, AttributeDescriptor>, Error>>()?;
        Ok(Self { attributes })
    }

    pub fn coerce(&self, key: &str, value: AnyObject, guard: &mut HeapGuard) -> Result<AvromaticValue, Error> {
        self.attributes.get(key)
            .ok_or_else(|| AvromaticError::InvalidAttribute { name: key.to_string() }.into())
            .and_then(|descriptor| descriptor.coerce(value, guard))
    }

    pub fn to_ruby(&self, key: &str, value: &AvromaticValue) -> Option<AnyObject> {
        self.attributes.get(key)
            .map(|descriptor| descriptor.to_ruby(value))
    }

    pub fn serialize<'a, I>(&self, values: &HashMap<String, AvromaticValue>, schema: I)
                            -> Result<AvroValue, Error>
        where I: SchemaIter<'a> + 'a
    {
        let schema = schema.record_schema().unwrap();
        let mut record = schema.new_record();
        schema.fields().iter().map(|field| {
            let value = values.get(field.name()).unwrap_or(&AvromaticValue::Null);
            let attribute = &self.attributes[field.name()];
            record.put(field.name(), attribute.serialize(value, field.schema())?);
            Ok(())
        }).collect::<Result<(), Error>>()?;
        Ok(record.avro())
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
            }
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
    pub fn build<'a>(
        field_schema: SchemaRef<'a>,
        default: Option<AvroValue>,
        nested_models: &ModelRegistry,
    ) -> Result<Self, Error> {
        let type_descriptor = TypeDescriptor::build(field_schema, nested_models)?;
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
        } else if let TypeDescriptor::Union(_, ref variants) = self.type_descriptor {
            variants.len() == 2
                && variants[0] == TypeDescriptor::Null
                && variants[1] == TypeDescriptor::Boolean
        } else {
            false
        }
    }

    pub fn serialize<'a, I>(&self, value: &AvromaticValue, schema: I)
                            -> Result<AvroValue, Error>
        where I: SchemaIter<'a> + 'a
    {
        self.type_descriptor.serialize(value, schema)
    }
}

#[derive(Debug, PartialEq)]
enum TypeDescriptor {
    Boolean,
    Enum(Vec<String>),
    Fixed(usize),
    Float,
    Double,
    Int,
    Long,
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

    Custom(CustomTypeConfiguration, Box<TypeDescriptor>),
}

impl TypeDescriptor {
    pub fn build(schema: SchemaRef, nested_models: &ModelRegistry) -> Result<Self, Error> {
        let out = match schema.kind() {
            SchemaKind::Null => TypeDescriptor::Null,
            SchemaKind::Boolean => TypeDescriptor::Boolean,
            SchemaKind::Int => TypeDescriptor::Int,
            SchemaKind::Long => TypeDescriptor::Long,
            SchemaKind::Float => TypeDescriptor::Float,
            SchemaKind::Double => TypeDescriptor::Double,
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
            }
            SchemaKind::Array => {
                let schema = schema.array_schema()
                    .ok_or_else(|| format_err!("Invalid Schema"))?;
                let inner = TypeDescriptor::build(schema, nested_models)?;
                TypeDescriptor::Array(Box::new(inner))
            }
            SchemaKind::Map => {
                let schema = schema.map_schema()
                    .ok_or_else(|| format_err!("Invalid Schema"))?;
                let inner = TypeDescriptor::build(schema, nested_models)?;
                TypeDescriptor::Map(Box::new(inner))
            }
            SchemaKind::Union => {
                let union_schema = schema.union_schema()
                    .ok_or_else(|| format_err!("Invalid Schema"))?;
                let maybe_null_index = union_schema.variants().iter().position(|variant| {
                    variant.kind() == SchemaKind::Null
                });
                if maybe_null_index.map(|x| x != 0).unwrap_or(false) {
                    return Err(format_err!("a null type in a union must be the first member"));
                }
                let variants = union_schema
                    .variants()
                    .into_iter()
                    .map(|variant| TypeDescriptor::build(variant, nested_models))
                    .collect::<Result<Vec<TypeDescriptor>, Error>>()?;
                TypeDescriptor::Union(union_schema.union_ref_map(), variants)
            }
            SchemaKind::Record => {
                let inner = AvromaticModel::build_model(
                    schema.as_full_schema(),
                    nested_models,
                );
                TypeDescriptor::Record(inner)
            }
            SchemaKind::Enum => {
                let symbols = schema.enum_symbols()
                    .ok_or_else(|| format_err!("Invalid Schema"))?;
                TypeDescriptor::Enum(symbols.to_vec())
            }
        };

        if let Some(fullname) = schema.fullname() {
            if let Some(custom_type) = CustomTypeRegistry::global().fetch(&fullname) {
                return Ok(TypeDescriptor::Custom(custom_type, Box::new(out)));
            }
        }

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
            TypeDescriptor::Int => coerce_integer(value, guard),
            TypeDescriptor::Long => coerce_long(value, guard),
            TypeDescriptor::Date => coerce_date(value, guard),
            TypeDescriptor::TimestampMillis => coerce_timestamp_millis(value, guard),
            TypeDescriptor::TimestampMicros => coerce_timestamp_micros(value, guard),
            TypeDescriptor::Float => coerce_float(value, guard),
            TypeDescriptor::Double => coerce_double(value, guard),
            TypeDescriptor::Fixed(length) => coerce_fixed(value, *length, guard),
            TypeDescriptor::Array(inner) => coerce_array(value, inner, guard),
            TypeDescriptor::Union(_, variants) => coerce_union(value, variants, guard),
            TypeDescriptor::Record(inner) => coerce_record(value, inner, guard),
            TypeDescriptor::Map(inner) => coerce_map(value, inner, guard),
            TypeDescriptor::Custom(custom, _) => coerce_custom(value, custom, guard),
        }
    }

    pub fn to_ruby(&self, value: &AvromaticValue) -> AnyObject {
        match (value, self) {
            (AvromaticValue::Null, _) => NilClass::new().into(),
            (AvromaticValue::True, _) => Boolean::new(true).to_any_object(),
            (AvromaticValue::False, _) => Boolean::new(false).to_any_object(),
            (AvromaticValue::String(string), _) => string.to_any_object(),
            (AvromaticValue::Long(n), TypeDescriptor::Date) => RDate::from_integer(n),
            (AvromaticValue::Long(n), TypeDescriptor::TimestampMillis) => RTime::from_millis(n),
            (AvromaticValue::Long(n), TypeDescriptor::TimestampMicros) => RTime::from_micros(n),
            (AvromaticValue::Long(n), _) => n.to_any_object(),
            (AvromaticValue::Float(f), _) => f.to_any_object(),
            (AvromaticValue::Array(values), TypeDescriptor::Array(inner)) => {
                values.iter().map(|v| inner.to_ruby(v)).collect::<Array>().to_any_object()
            }
            (AvromaticValue::Union(index, value), TypeDescriptor::Union(_, variants)) => {
                variants[*index].to_ruby(value)
            }
            (AvromaticValue::Record(value), _) => {
                value.to_any_object()
                // let model = unsafe { value.to::<AvromaticModelAttributes>() };
                // model.attribute_hash_for_value().into()
            }
            (AvromaticValue::Map(value), TypeDescriptor::Map(inner)) => {
                let mut hash = Hash::new();
                value.iter().for_each(|(k, v)| {
                    hash.store(RString::new_utf8(k), inner.to_ruby(v));
                });
                hash.to_any_object()
            }
            (AvromaticValue::Custom(value), TypeDescriptor::Custom(_, _)) => value.to_any_object(),
            _ => unreachable!(),
        }
    }

    fn mark(&self) {
        match self {
            TypeDescriptor::Array(inner) => inner.mark(),
            TypeDescriptor::Map(inner) => inner.mark(),
            TypeDescriptor::Union(_, variants) => variants.iter().for_each(TypeDescriptor::mark),
            TypeDescriptor::Record(inner) => GC::mark(inner),
            TypeDescriptor::Custom(custom_type, default) => {
                GC::mark(custom_type);
                default.mark();
            }
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
                let mut rstring = RString::from_bytes(&bytes, &encoding);
                rstring.freeze();
                guard.guard(rstring.to_any_object());
                AvromaticValue::String(rstring)
            }
            (TypeDescriptor::Fixed(length), AvroValue::String(s)) if s.len() == *length => {
                let mut rstring = RString::new_utf8(s);
                rstring.freeze();
                guard.guard(rstring.to_any_object());
                AvromaticValue::String(rstring)
            }
            (TypeDescriptor::Enum(_), AvroValue::Enum(_, s)) |
            (TypeDescriptor::Enum(_), AvroValue::String(s)) |
            (TypeDescriptor::String, AvroValue::String(s)) => {
                let mut rstring = RString::new_utf8(s);
                rstring.freeze();
                guard.guard(rstring.to_any_object());
                AvromaticValue::String(rstring)
            }
            (TypeDescriptor::Int, AvroValue::Int(n)) |
            (TypeDescriptor::Date, AvroValue::Date(n)) => {
                AvromaticValue::Long((*n as i64).into())
            }
            (TypeDescriptor::Int, AvroValue::Long(n)) => {
                // TODO: Will this truncate or roll over? Maybe tests are in order.
                AvromaticValue::Long((*n as i32).into())
            }
            (TypeDescriptor::Long, AvroValue::Long(n)) |
            (TypeDescriptor::TimestampMicros, AvroValue::TimestampMicros(n)) |
            (TypeDescriptor::TimestampMillis, AvroValue::TimestampMillis(n)) => {
                AvromaticValue::Long((*n).into())
            }
            (TypeDescriptor::Float, AvroValue::Float(n)) => {
                let f = Float::new(*n as f64);
                guard.guard(f.to_any_object());
                AvromaticValue::Float(f)
            }
            (TypeDescriptor::Double, AvroValue::Double(n)) => {
                let f = Float::new(*n);
                guard.guard(f.to_any_object());
                AvromaticValue::Float(f)
            }
            (TypeDescriptor::Map(inner), AvroValue::Map(values)) => {
                let map = values.into_iter()
                    .map(|(k, v)| Ok((k.to_string(), inner.avro_to_attribute(v, guard)?)))
                    .collect::<Result<HashMap<String, AvromaticValue>, Error>>()?;
                AvromaticValue::Map(map)
            }
            (TypeDescriptor::Array(inner), AvroValue::Array(values)) => {
                let attributes = values.into_iter()
                    .map(|v| inner.avro_to_attribute(v, guard))
                    .collect::<Result<Vec<AvromaticValue>, Error>>()?;
                AvromaticValue::Array(attributes)
            }
            (TypeDescriptor::Union(ref_index, schemas), AvroValue::Union(union_ref, value)) => {
                if let Some(index) = ref_index.get(&union_ref) {
                    let value = schemas[*index].avro_to_attribute(value, guard)?;
                    AvromaticValue::Union(*index, Box::new(value))
                } else {
                    unimplemented!()
                }
            }
            (TypeDescriptor::Union(_, schemas), value) => {
                for schema in schemas {
                    if let Ok(value) = schema.avro_to_attribute(value, guard) {
                        return Ok(value);
                    }
                }
                return Err(format_err!("Failed to convert avro '{:?}' to {:?}", value, self));
            }
            (TypeDescriptor::Record(inner), value) => {
                let schema = inner.protect_public_send("_schema", &[]).unwrap();
                let descriptor = schema.get_data(&*MODEL_DESCRIPTOR_WRAPPER);
                let record = descriptor.avro_to_model(inner, value, guard)?;
                guard.guard(record.to_any_object());
                AvromaticValue::Record(record)
            }
            (TypeDescriptor::Custom(inner, default), value) => {
                let raw = default.avro_to_attribute(value, guard)?;
                let rb_raw = default.to_ruby(&raw);
                let custom_value = inner.deserialize(rb_raw);
                AvromaticValue::Custom(custom_value)
            }
            _ => return Err(format_err!("Failed to convert avro '{:?}' to {:?}", value, self)),
        };
        Ok(out)
    }

    pub fn serialize<'a, I>(&self, value: &AvromaticValue, schema: I)
                            -> Result<AvroValue, Error>
        where I: SchemaIter<'a> + 'a
    {
        let out = match (self, value) {
            (TypeDescriptor::Null, AvromaticValue::Null) => AvroValue::Null,
            (TypeDescriptor::Boolean, AvromaticValue::True) => AvroValue::Boolean(true),
            (TypeDescriptor::Boolean, AvromaticValue::False) => AvroValue::Boolean(false),
            (TypeDescriptor::String, AvromaticValue::String(rstring)) => serialize_string(rstring),
            (TypeDescriptor::Bytes, AvromaticValue::String(rstring)) => serialize_bytes(rstring),
            (TypeDescriptor::Enum(symbols), AvromaticValue::String(rstring)) =>
                serialize_enum(rstring, symbols)?,
            (TypeDescriptor::Int, AvromaticValue::Long(integer))
            if schema.kind() == SchemaKind::Int =>
                serialize_integer(integer),
            (TypeDescriptor::Long, AvromaticValue::Long(integer))
            if schema.kind() == SchemaKind::Long =>
                serialize_long(integer),
            (TypeDescriptor::Date, AvromaticValue::Long(integer)) => serialize_date(integer),
            (TypeDescriptor::TimestampMillis, AvromaticValue::Long(value)) =>
                serialize_timestamp_millis(value),
            (TypeDescriptor::TimestampMicros, AvromaticValue::Long(value)) =>
                serialize_timestamp_micros(value),
            (TypeDescriptor::Float, AvromaticValue::Float(float))
            if schema.kind() == SchemaKind::Float =>
                serialize_float(float),
            (TypeDescriptor::Double, AvromaticValue::Float(float))
            if schema.kind() == SchemaKind::Double =>
                serialize_double(float),
            (TypeDescriptor::Fixed(length), AvromaticValue::String(value)) =>
                serialize_fixed(value, *length),
            (TypeDescriptor::Array(inner), AvromaticValue::Array(value)) =>
                serialize_array(value, inner, schema)?,
            (TypeDescriptor::Union(_, variants), AvromaticValue::Union(index, value)) =>
                serialize_union(value, *index, variants, schema)?,
            (TypeDescriptor::Union(_, variants), value) =>
                serialize_untracked_union(value, variants, schema)?,
            (TypeDescriptor::Record(inner), AvromaticValue::Record(object)) =>
                serialize_record(object, inner)?,
            (TypeDescriptor::Map(inner), AvromaticValue::Map(value)) =>
                serialize_map(value, inner, schema)?,
            (TypeDescriptor::Custom(custom, default), AvromaticValue::Custom(value)) =>
                serialize_custom(value, custom, default, schema)?,
            _ => return Err(format_err!("bad to avro: {:?} {:?}", value, schema.schema())),
        };
        Ok(out)
    }

    pub fn decode<R: Read>(&self, reader: &mut R)
                           -> Result<AnyObject, Error>
    {
        match self {
            TypeDescriptor::Null => Ok(NilClass::new().into()),
            TypeDescriptor::Boolean => {
                let mut buf = [0u8; 1];
                reader.read_exact(&mut buf[..])?;

                match buf[0] {
                    0u8 => Ok(Boolean::new(false).into()),
                    1u8 => Ok(Boolean::new(true).into()),
                    _ => Err(DecodeError::new("not a bool").into()),
                }
            }
            TypeDescriptor::Int => decode_int(reader),
            TypeDescriptor::Date => zag_i32(reader).map(|i| RDate::from_i64(i as i64)),
            TypeDescriptor::Long => decode_long(reader),
            TypeDescriptor::TimestampMillis => zag_i64(reader).map(|i| RTime::from_i64_millis(i)),
            TypeDescriptor::TimestampMicros => zag_i64(reader).map(|i| RTime::from_i64_micros(i)),
            TypeDescriptor::Float => {
                let mut buf = [0u8; 4];
                reader.read_exact(&mut buf[..])?;
                Ok(Float::new(unsafe { transmute::<[u8; 4], f32>(buf) } as f64).into())
            }
            TypeDescriptor::Double => {
                let mut buf = [0u8; 8];
                reader.read_exact(&mut buf[..])?;
                Ok(Float::new(unsafe { transmute::<[u8; 8], f64>(buf) }).into())
            }
            TypeDescriptor::Bytes => {
                let len = decode_len(reader)?;
                let mut buf = Vec::with_capacity(len);
                unsafe {
                    buf.set_len(len);
                }
                reader.read_exact(&mut buf)?;
                let encoding = Encoding::find("ASCII-8BIT").unwrap();
                let mut rstring = RString::from_bytes(&buf, &encoding);
                rstring.freeze();
                Ok(rstring.into())
            }
            TypeDescriptor::String => {
                let len = decode_len(reader)?;
                let mut buf = Vec::with_capacity(len);
                unsafe {
                    buf.set_len(len);
                }
                reader.read_exact(&mut buf)?;

                std::str::from_utf8(&buf)
                    .map(|s| RString::new_utf8(s).into())
                    .map_err(|_| DecodeError::new("not a valid utf-8 string").into())
            }
            TypeDescriptor::Fixed(size) => {
                let mut buf = vec![0u8; *size as usize];
                reader.read_exact(&mut buf)?;
                let encoding = Encoding::find("ASCII-8BIT").unwrap();
                let mut rstring = RString::from_bytes(&buf, &encoding);
                rstring.freeze();
                Ok(rstring.into())
            }
            TypeDescriptor::Array(ref inner) => {
                let mut items = Array::new();

                loop {
                    let len = decode_len(reader)?;
                    // arrays are 0-terminated, 0i64 is also encoded as 0 in Avro
                    // reading a length of 0 means the end of the array
                    if len == 0 {
                        break;
                    }

                    for _ in 0..len {
                        items.push(inner.decode(reader)?);
                    }
                }

                Ok(items.into())
            }
            TypeDescriptor::Map(ref inner) => {
                let mut items = Hash::new();

                loop {
                    let len = decode_len(reader)?;
                    // maps are 0-terminated, 0i64 is also encoded as 0 in Avro
                    // reading a length of 0 means the end of the map
                    if len == 0 {
                        break;
                    }

                    for _ in 0..len {
                        let mut key = TypeDescriptor::String.decode(reader)
                            .map_err(|_| DecodeError::new("map key is not a string"))?;
                        key.freeze();
                        let value = inner.decode(reader)?;
                        items.store(key, value);
                    }
                }

                Ok(items.into())
            }
            TypeDescriptor::Union(_, variants) => {
                let index = zag_i64(reader)?;
                match variants.get(index as usize) {
                    Some(variant) => {
                        // TODO tag the value somehow
                        variant.decode(reader)
                    }
                    None => Err(DecodeError::new("Union index out of bounds").into()),
                }
            }
            TypeDescriptor::Record(ref record) => {
                // TODO delegate to record class
                unimplemented!()
            }
            TypeDescriptor::Enum(symbols) => {
                let index = zag_i32(reader)?;
                if index >= 0 && (index as usize) <= symbols.len() {
                    let symbol = &symbols[index as usize];
                    Ok(RString::new_utf8(&symbol).into())
                } else {
                    Err(DecodeError::new("enum symbol index out of bounds").into())
                }
            }
            TypeDescriptor::Custom(inner, default) => {
                let raw = default.decode(reader)?;
                Ok(inner.deserialize(raw))
            }
        }
    }
}

fn bad_coercion(value: &AnyObject, name: &str) -> Error {
    AvromaticError::InvalidValue {
        value: value.protect_public_send("inspect", &[])
            .expect("unexpected exception")
            .try_convert_to::<RString>().unwrap().to_string(),
        name: name.to_string(),
    }.into()
}

fn coerce_null(value: &AnyObject) -> Result<AvromaticValue, Error> {
    match value.is_nil() {
        true => Ok(AvromaticValue::Null),
        false => Err(bad_coercion(value, "null"))
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
                .and_then(|symbol| symbol.protect_public_send("to_s", &[]).expect("unexpected exception").try_convert_to())
        })
        .map(|mut s| s.freeze())
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
        .and_then(|mut rstring| if rstring.to_str().len() != length {
            None
        } else {
            rstring.freeze();
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
        return Err(bad_coercion(value, "date"));
    };
    guard.guard(n.to_any_object());
    Ok(AvromaticValue::Long(n))
}

fn coerce_timestamp_millis(value: &AnyObject, guard: &mut HeapGuard) -> Result<AvromaticValue, Error> {
    if instance_of(value, Class::from_existing("Numeric")) {
        let n = value.protect_public_send("to_i", &[])
            .expect("unexpected exception")
            .try_convert_to::<Integer>()
            .map_err(|_| bad_coercion(value, "timestamp-millis"))?;
        guard.guard(n.to_any_object());
        return Ok(AvromaticValue::Long(n));
    }

    if value.class() == Class::from_existing("Date") {
        return Err(bad_coercion(value, "timestamp-millis"));
    }

    let n = value.protect_public_send("to_time", &[])
        .expect("unexpected exception")
        .try_convert_to::<RTime>()
        .map(|time| time.to_millis())
        .map_err(|_| bad_coercion(value, "timestamp-millis"))?;
    guard.guard(n.to_any_object());
    Ok(AvromaticValue::Long(n))
}

fn coerce_timestamp_micros(value: &AnyObject, guard: &mut HeapGuard) -> Result<AvromaticValue, Error> {
    if instance_of(value, Class::from_existing("Numeric")) {
        let n = value.protect_public_send("to_i", &[])
            .expect("unexpected exception")
            .try_convert_to::<Integer>()
            .map_err(|_| bad_coercion(value, "timestamp-micros"))?;
        guard.guard(n.to_any_object());
        return Ok(AvromaticValue::Long(n));
    }

    if value.class() == Class::from_existing("Date") {
        return Err(bad_coercion(value, "timestamp-micros"));
    }

    let n = value.protect_public_send("to_time", &[])
        .expect("unexpected exception")
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

fn coerce_long(value: &AnyObject, guard: &mut HeapGuard) -> Result<AvromaticValue, Error> {
    value.try_convert_to::<Integer>()
        .map(|int| {
            guard.guard(int.to_any_object());
            int
        })
        .map(AvromaticValue::Long)
        .or_else(|_| Err(bad_coercion(value, "integer")))
}

fn coerce_double(value: &AnyObject, guard: &mut HeapGuard) -> Result<AvromaticValue, Error> {
    value.try_convert_to::<Float>()
        .or_else(|_| value.try_convert_to::<Integer>().map(|i| Float::new(i.to_i64() as f64)))
        .map(|float| {
            guard.guard(float.to_any_object());
            float
        })
        .map(AvromaticValue::Float)
        .map_err(|_| bad_coercion(value, "float"))
}

fn coerce_float(value: &AnyObject, guard: &mut HeapGuard) -> Result<AvromaticValue, Error> {
    value.try_convert_to::<Float>()
        .or_else(|_| value.try_convert_to::<Integer>().map(|i| Float::new(i.to_i64() as f64)))
        .map(|float| {
            guard.guard(float.to_any_object());
            float
        })
        .map(AvromaticValue::Float)
        .map_err(|_| bad_coercion(value, "float"))
}

fn coerce_enum(value: &AnyObject, symbols: &[String], guard: &mut HeapGuard) -> Result<AvromaticValue, Error> {
    convert_string(value)
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
        return Ok(AvromaticValue::Record(value.to_any_object()));
    }
    let record = record_class.protect_public_send("new", &[value.to_any_object()])
        .expect("unexpected exception");
    guard.guard(record.to_any_object());
    Ok(AvromaticValue::Record(record))
}

fn coerce_custom(
    value: &AnyObject,
    custom_type: &CustomTypeConfiguration,
    guard: &mut HeapGuard,
) -> Result<AvromaticValue, Error>
{
    let coerced = custom_type.deserialize(value.to_any_object());
    Ok(AvromaticValue::Custom(coerced))
}

fn serialize_string(rstring: &RString) -> AvroValue {
    AvroValue::String(rstring.to_string())
}

fn serialize_bytes(rstring: &RString) -> AvroValue {
    AvroValue::Bytes(rstring.to_vec_u8_unchecked())
}

fn serialize_enum(rstring: &RString, symbols: &[String]) -> Result<AvroValue, Error> {
    let value = rstring.to_str();
    let position = symbols.iter().position(|symbol| symbol == value);
    position
        .map(|index| AvroValue::Enum(index as i32, value.to_string()))
        .ok_or_else(|| format_err!("failed to serialize {:?} to enum[{:?}]", value, symbols))
}

fn serialize_integer(value: &Integer) -> AvroValue {
    AvroValue::Int(value.to_i64() as i32)
}

fn serialize_long(value: &Integer) -> AvroValue {
    AvroValue::Long(value.to_i64())
}

fn serialize_date(value: &Integer) -> AvroValue {
    AvroValue::Date(value.to_i64() as i32)
}

fn serialize_timestamp_millis(value: &Integer) -> AvroValue {
    AvroValue::TimestampMillis(value.to_i64())
}

fn serialize_timestamp_micros(value: &Integer) -> AvroValue {
    AvroValue::TimestampMicros(value.to_i64())
}

fn serialize_float(value: &Float) -> AvroValue {
    AvroValue::Float(value.to_f64() as f32)
}

fn serialize_double(value: &Float) -> AvroValue {
    AvroValue::Double(value.to_f64())
}

fn serialize_fixed(rstring: &RString, size: usize) -> AvroValue {
    AvroValue::Fixed(size, rstring.to_vec_u8_unchecked())
}

fn serialize_array<'a, I>(value: &[AvromaticValue], inner: &TypeDescriptor, schema: I)
                          -> Result<AvroValue, Error>
    where I: SchemaIter<'a> + 'a
{
    let schema = schema.array_schema().unwrap();
    let values = value
        .iter()
        .map(|v| inner.serialize(v, schema))
        .collect::<Result<Vec<AvroValue>, Error>>()?;
    Ok(AvroValue::Array(values))
}

fn serialize_union<'a, I>(
    value: &AvromaticValue,
    index: usize,
    variants: &[TypeDescriptor],
    schema: I,
) -> Result<AvroValue, Error>
    where I: SchemaIter<'a> + 'a
{
    let schema = schema.union_schema().unwrap().variants()[index];
    let union_ref = UnionRef::from_schema(schema.schema());
    let union_value = variants[index].serialize(value, schema)?;
    Ok(AvroValue::Union(union_ref, Box::new(union_value)))
}

fn serialize_untracked_union<'a, I>(
    value: &AvromaticValue,
    variants: &[TypeDescriptor],
    schema: I,
) -> Result<AvroValue, Error>
    where I: SchemaIter<'a> + 'a
{
    schema.union_schema()
        .unwrap()
        .variants()
        .into_iter()
        .enumerate()
        .map(|(i, variant)| {
            let descriptor = &variants[i];
            let val = Box::new(descriptor.serialize(value, variant)?);
            Ok(AvroValue::Union(UnionRef::from_schema(variant.schema()), val))
        })
        .find(Result::is_ok)
        .unwrap_or_else(|| Err(format_err!("Invalid union")))
}

fn serialize_record(object: &AnyObject, inner: &Class) -> Result<AvroValue, Error> {
    let mut attributes = object.instance_variable_get("@_attributes");
    let storage = attributes.get_data_mut(&*MODEL_STORAGE_WRAPPER);
    let object = crate::util::class_ancestor_send(&inner, "_schema");
    let descriptor = object.get_data(&*MODEL_DESCRIPTOR_WRAPPER);
    descriptor.value.descriptor.serialize(&storage.attributes, &descriptor.value.schema)
}

fn serialize_map<'a, I>(value: &HashMap<String, AvromaticValue>, inner: &TypeDescriptor, schema: I)
                        -> Result<AvroValue, Error>
    where I: SchemaIter<'a> + 'a
{
    let schema = schema.map_schema().unwrap();
    let values = value
        .iter()
        .map(|(k, v)| Ok((k.to_string(), inner.serialize(v, schema)?)))
        .collect::<Result<HashMap<String, AvroValue>, Error>>()?;
    Ok(AvroValue::Map(values))
}

fn serialize_custom<'a, I>(
    value: &AnyObject,
    custom: &CustomTypeConfiguration,
    default: &TypeDescriptor,
    schema: I,
) -> Result<AvroValue, Error>
    where I: SchemaIter<'a> + 'a
{
    let serialized = custom.serialize(value.to_any_object());
    let mut guard = HeapGuard::new();
    guard.guard(serialized.to_any_object());
    let value = default.coerce(&serialized, &mut guard)?;
    default.serialize(&value, schema)
}

pub fn initialize() {
    Class::new("ModelDescriptor", None).define(|_| ());
}

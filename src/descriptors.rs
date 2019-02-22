use avro_rs::{
    FullSchema,
    Schema,
    schema::{RecordField, SchemaKind, SchemaIter, SchemaRef, UnionRef},
    types::Value as AvroValue,
};
use crate::heap_guard::HeapGuard;
use crate::model::{AvromaticModel, ModelStorage, MODEL_STORAGE_WRAPPER};
use crate::serializer;
use crate::values::AvromaticValue;
use failure::{Error, Fail, format_err};
use rutie::*;
use std::collections::HashMap;

#[derive(Debug, Fail)]
enum AvromaticError {
    #[fail(display = "attribute '{}' does not exist", name)]
    InvalidAttribute {
        name: String,
    },
    #[fail(display = "cannot coerce '{}' to '{}'", value, name)]
    InvalidValue {
        value: String,
        name: String,
    },
}

pub struct ModelDescriptorInner {
    schema: FullSchema,
    descriptor: RecordDescriptor,
}

wrappable_struct!(
    ModelDescriptorInner,
    ModelDescriptorWrapper,
    MODEL_DESCRIPTOR_WRAPPER,
    mark(value) {
        value.descriptor.mark();
    }
);

impl ModelDescriptorInner {
    pub fn new(schema: FullSchema) -> Result<Self, Error> {
        let descriptor = RecordDescriptor::build(&schema)?;
        Ok(Self { schema, descriptor })
    }

    pub fn coerce(&self, key: &str, value: AnyObject, guard: &mut HeapGuard) -> Result<AvromaticValue, Error> {
        self.descriptor.coerce(key, value, guard)
    }

    pub fn serialize(&self, attributes: &HashMap<String, AvromaticValue>)
        -> Result<Vec<u8>, Error>
    {
        serializer::serialize(&self.schema, attributes)
    }

    pub fn deserialize(&self, class: &Class, data: &[u8], guard: &mut HeapGuard) -> Result<AnyObject, Error> {
        let mut cursor = std::io::Cursor::new(data);
        // TODO: need to get writer schema
        let value = avro_rs::from_avro_datum(&self.schema, &mut cursor, None)?;
        self.avro_to_model(class, value, guard)
    }

    fn avro_to_model(&self, class: &Class, value: AvroValue, guard: &mut HeapGuard)
        -> Result<AnyObject, Error>
    {
        let attributes = self.descriptor.avro_to_attributes(value, guard)?;
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
        self.descriptor.attributes.iter().for_each(|(k, v)| f(k, v))
    }
}

class!(ModelDescriptor);

impl ModelDescriptor {
    pub fn new(schema: FullSchema) -> Result<Self, Error> {
        let inner = ModelDescriptorInner::new(schema)?;
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
            Ok((field.name().to_string(), AttributeDescriptor::build(field.schema())?))
        }).collect::<Result<HashMap<String, AttributeDescriptor>, Error>>()?;
        Ok(Self { attributes })
    }

    pub fn coerce(&self, key: &str, value: AnyObject, guard: &mut HeapGuard) -> Result<AvromaticValue, Error> {
        self.attributes.get(key)
            .ok_or_else(|| AvromaticError::InvalidAttribute{ name: key.to_string() }.into())
            .and_then(|descriptor| descriptor.coerce(value, guard))
    }

    fn mark(&self) {
        self.attributes.values().for_each(AttributeDescriptor::mark);
    }

    fn avro_to_attributes(&self, value: AvroValue, guard: &mut HeapGuard)
        -> Result<HashMap<String, AvromaticValue>, Error>
    {
        match value {
            AvroValue::Record(fields) => {
                let mut attributes = HashMap::new();
                fields.into_iter().map(|(key, value)| {
                    let descriptor = self.attributes.get(&key)
                        .unwrap();
                    let attribute = descriptor.avro_to_attribute(value, guard)?;
                    attributes.insert(key, attribute);
                    Ok(())
                }).collect::<Result<Vec<()>, Error>>()?;
                Ok(attributes)
            },
            _ => unimplemented!(),
        }
    }
}

#[derive(Debug)]
pub struct AttributeDescriptor {
    type_descriptor: TypeDescriptor,
    default: Option<AvromaticValue>,
}

impl AttributeDescriptor {
    pub fn build<'a>(field_schema: SchemaRef<'a>) -> Result<Self, Error> {
        Ok(
            Self {
                type_descriptor: TypeDescriptor::build(field_schema)?,
                default: None,
            }
        )
    }

    pub fn coerce(&self, value: AnyObject, guard: &mut HeapGuard) -> Result<AvromaticValue, Error> {
        self.type_descriptor.coerce(&value, guard)
    }

    pub fn default(&self) -> AnyObject {
        NilClass::new().into()
    }

    fn mark(&self) {
        self.type_descriptor.mark();
        if let Some(v) = &self.default {
            v.mark();
        }
    }

    fn avro_to_attribute(&self, value: AvroValue, guard: &mut HeapGuard)
        -> Result<AvromaticValue, Error>
    {
        self.type_descriptor.avro_to_attribute(value, guard)
    }
}

#[derive(Debug)]
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
                    .collect()?;
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
        match self {
            TypeDescriptor::Null => coerce_null(value),
            TypeDescriptor::Boolean => coerce_boolean(value),
            TypeDescriptor::String => coerce_string(value, guard),
            TypeDescriptor::Bytes => coerce_string(value, guard),
            TypeDescriptor::Enum(symbols) => coerce_enum(value, symbols, guard),
            TypeDescriptor::Integer => coerce_integer(value, guard),
            TypeDescriptor::Float => coerce_float(value, guard),
            TypeDescriptor::Fixed(length) => coerce_fixed(value, *length, guard),
            TypeDescriptor::Array(inner) => coerce_array(value, inner, guard),
            TypeDescriptor::Union(_, variants) => coerce_union(value, variants, guard),
            TypeDescriptor::Record(inner) => coerce_record(value, inner, guard),
            _ => unimplemented!(),
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

    fn avro_to_attribute(&self, value: AvroValue, guard: &mut HeapGuard)
        -> Result<AvromaticValue, Error>
    {
        let out = match self {
            TypeDescriptor::Null => AvromaticValue::Null,
            TypeDescriptor::String => {
                if let AvroValue::String(s) = value {
                    let rstring: RString = s.into();
                    guard.guard(rstring.to_any_object());
                    AvromaticValue::String(rstring)
                } else {
                    unimplemented!()
                }
            },
            TypeDescriptor::Integer => {
                match value {
                    AvroValue::Int(n) => AvromaticValue::Long((n as i64).into()),
                    AvroValue::Long(n) => AvromaticValue::Long(n.into()),
                    _ => unimplemented!()
                }
            },
            TypeDescriptor::Fixed(usize) => {
                if let AvroValue::Fixed(_, bytes) = value {
                    let encoding = Encoding::find("ASCII-8BIT").unwrap();
                    let rstring = RString::from_bytes(&bytes, &encoding);
                    guard.guard(rstring.to_any_object());
                    AvromaticValue::String(rstring)
                } else {
                    unimplemented!()
                }
            },
            TypeDescriptor::Array(inner) => {
                if let AvroValue::Array(values) = value {
                    let attributes = values.into_iter()
                        .map(|v| inner.avro_to_attribute(v, guard))
                        .collect::<Result<Vec<AvromaticValue>, Error>>()?;
                    AvromaticValue::Array(attributes)
                } else {
                    unimplemented!()
                }
            },
            TypeDescriptor::Union(ref_index, schemas) => {
                if let AvroValue::Union(union_ref, value) = value {
                    if let Some(index) = ref_index.get(&union_ref) {
                        let value = schemas[*index].avro_to_attribute(*value, guard)?;
                        AvromaticValue::Union(*index, Box::new(value))
                    } else {
                        unimplemented!()
                    }
                } else {
                    unimplemented!()
                }
            },
            TypeDescriptor::Record(inner) => {
                let schema = inner.send("_schema", None);
                let descriptor = schema.get_data(&*MODEL_DESCRIPTOR_WRAPPER);
                let record = descriptor.avro_to_model(inner, value, guard)?;
                guard.guard(record.to_any_object());
                AvromaticValue::Record(record)
            }
            _ => unimplemented!(),
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

fn coerce_string(value: &AnyObject, guard: &mut HeapGuard) -> Result<AvromaticValue, Error> {
    value.try_convert_to::<RString>()
        .or_else(|_| {
            value.try_convert_to::<Symbol>()
                .and_then(|symbol| symbol.send("to_s", None).try_convert_to())
        })
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
        .map(|float| {
            guard.guard(float.to_any_object());
            float
        })
        .map(AvromaticValue::Float)
        .or_else(|_| Err(bad_coercion(value, "float")))
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
    let out = variants.iter()
        .enumerate()
        .map(|(i, variant)| Ok(AvromaticValue::Union(i, Box::new(variant.coerce(&value, guard)?))))
        .find(Result::is_ok)
        .unwrap_or_else(|| Err(bad_coercion(value, "union")));
    out
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

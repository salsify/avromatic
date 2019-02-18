use avro_rs::{
    FullSchema,
    Schema,
    schema::{RecordField, SchemaKind, SchemaIter, SchemaRef},
};
use crate::model::AvromaticModel;
use crate::serializer;
use crate::values::AvromaticValue;
use failure::{Error, Fail};
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
    #[fail(display = "fuck")]
    Fuck
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
    pub fn new(schema: FullSchema) -> Self {
        let descriptor = RecordDescriptor::build(&schema);
        Self { schema, descriptor }
    }

    pub fn coerce(&self, key: &str, value: AnyObject) -> Result<AvromaticValue, Error> {
        self.descriptor.coerce(key, value)
    }

    pub fn serialize(&self, attributes: &HashMap<String, AvromaticValue>)
        -> Result<Vec<u8>, Error>
    {
        serializer::serialize(&self.schema, attributes)
    }

    pub fn each_field<F>(&self, mut f: F)
        where F: FnMut(&str, &AttributeDescriptor)
    {
        self.descriptor.attributes.iter().for_each(|(k, v)| f(k, v))
    }
}

class!(ModelDescriptor);

impl ModelDescriptor {
    pub fn new(schema: FullSchema) -> Self {
        let inner = ModelDescriptorInner::new(schema);
        Class::from_existing("ModelDescriptor")
            .wrap_data(inner, &*MODEL_DESCRIPTOR_WRAPPER)
    }
}

#[derive(Debug)]
struct RecordDescriptor {
    attributes: HashMap<String, AttributeDescriptor>,
}

impl RecordDescriptor {
    pub fn build(schema: &FullSchema) -> Self {
        let record = schema.record_schema();
        let attributes = record.fields().iter().map(|field| {
            (field.name().to_string(), AttributeDescriptor::build(field.schema()))
        }).collect();
        Self { attributes }
    }

    pub fn coerce(&self, key: &str, value: AnyObject) -> Result<AvromaticValue, Error> {
        self.attributes.get(key)
            .ok_or_else(|| AvromaticError::InvalidAttribute{ name: key.to_string() }.into())
            .and_then(|descriptor| descriptor.coerce(value))
    }

    fn mark(&self) {
        self.attributes.values().for_each(AttributeDescriptor::mark);
    }
}

#[derive(Debug)]
pub struct AttributeDescriptor {
    type_descriptor: TypeDescriptor,
    default: Option<AvromaticValue>,
}

impl AttributeDescriptor {
    pub fn build<'a>(field_schema: SchemaRef<'a>) -> Self {
        Self {
            type_descriptor: TypeDescriptor::build(field_schema),
            default: None,
        }
    }

    pub fn coerce(&self, value: AnyObject) -> Result<AvromaticValue, Error> {
        self.type_descriptor.coerce(&value)
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
}

#[derive(Debug)]
enum TypeDescriptor  {
    Boolean,
    Date,
    Enum,
    Fixed(usize),
    Float,
    Integer,
    Null,
    String,
    Bytes,
    TimestampMicros,
    TimestampMillis,

    Array(Box<TypeDescriptor>),
    Map(Box<TypeDescriptor>),
//     Custom,
    Record(Class),
    Union(Vec<TypeDescriptor>),
}

impl TypeDescriptor {
    pub fn build(schema: SchemaRef) -> Self {
        match schema.kind() {
            SchemaKind::Null => TypeDescriptor::Null,
            SchemaKind::Boolean => TypeDescriptor::Boolean,
            SchemaKind::Int => TypeDescriptor::Integer,
            SchemaKind::Long => TypeDescriptor::Integer,
            SchemaKind::Float => TypeDescriptor::Float,
            SchemaKind::Double => TypeDescriptor::Float,
            SchemaKind::Bytes => TypeDescriptor::Bytes,
            SchemaKind::String => TypeDescriptor::String,
            SchemaKind::Fixed => TypeDescriptor::Fixed(schema.fixed_size()),
            SchemaKind::Array => {
                let inner = TypeDescriptor::build(schema.array_schema());
                TypeDescriptor::Array(Box::new(inner))
            },
            SchemaKind::Map => {
                let inner = TypeDescriptor::build(schema.map_schema());
                TypeDescriptor::Map(Box::new(inner))
            },
            SchemaKind::Union => {
                let variants = schema
                    .union_schema()
                    .variants()
                    .into_iter()
                    .map(TypeDescriptor::build)
                    .collect();
                TypeDescriptor::Union(variants)
            },
            SchemaKind::Record => {
                let inner = AvromaticModel::build_model(schema.as_full_schema());
                TypeDescriptor::Record(inner)
            },
            SchemaKind::Enum => TypeDescriptor::Null,
        }
    }

    pub fn coerce(&self, value: &AnyObject) -> Result<AvromaticValue, Error> {
        match self {
            TypeDescriptor::Null => coerce_null(value),
            TypeDescriptor::String => coerce_string(value),
            TypeDescriptor::Integer => coerce_integer(value),
            TypeDescriptor::Fixed(length) => coerce_fixed(value, *length),
            TypeDescriptor::Array(inner) => coerce_array(value, inner),
            TypeDescriptor::Union(variants) => coerce_union(value, variants),
            TypeDescriptor::Record(inner) => coerce_record(value, inner),
            _ => Err(AvromaticError::Fuck.into()),
        }
    }

    fn mark(&self) {
        match self {
            TypeDescriptor::Array(inner) => inner.mark(),
            TypeDescriptor::Union(variants) => variants.iter().for_each(TypeDescriptor::mark),
            TypeDescriptor::Record(inner) => GC::mark(inner),
            _ => (),
        }
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
        Err(bad_coercion(value, "null"))
    }
}

fn coerce_string(value: &AnyObject) -> Result<AvromaticValue, Error> {
    value.try_convert_to::<RString>()
        .or_else(|_| {
            value.try_convert_to::<Symbol>()
                .and_then(|symbol| symbol.send("to_s", None).try_convert_to())
        })
        .map(AvromaticValue::String)
        .or_else(|_| Err(bad_coercion(value, "string")))
}

fn coerce_fixed(value: &AnyObject, length: usize) -> Result<AvromaticValue, Error> {
    value.try_convert_to::<RString>()
        .ok()
        .and_then(|rstring| if rstring.to_str().len() != length {
            None
        } else {
            Some(AvromaticValue::String(rstring))
        })
        .ok_or_else(|| bad_coercion(value, &format!("fixed({})", length)))
}

fn coerce_integer(value: &AnyObject) -> Result<AvromaticValue, Error> {
    value.try_convert_to::<Integer>()
        .map(AvromaticValue::Long)
        .or_else(|_| Err(bad_coercion(value, "integer")))
}

fn coerce_array(value: &AnyObject, inner: &TypeDescriptor)
    -> Result<AvromaticValue, Error>
{
    value.try_convert_to::<Array>()
        .map_err(|_| bad_coercion(value, "array"))
        .and_then(|array| array.into_iter().map(|element| inner.coerce(&element)).collect())
        .map(AvromaticValue::Array)
}

fn coerce_union(value: &AnyObject, variants: &[TypeDescriptor])
    -> Result<AvromaticValue, Error>
{
    let out = variants.iter()
        .enumerate()
        .map(|(i, variant)| Ok(AvromaticValue::Union(i, Box::new(variant.coerce(&value)?))))
        .find(Result::is_ok)
        .unwrap_or_else(|| Err(bad_coercion(value, "union")));
    out
}

fn coerce_record(value: &AnyObject, record_class: &Class)
    -> Result<AvromaticValue, Error>
{
    let valid_object = value.class().ancestors().iter().any(|class| class.is_eql(record_class));
    if valid_object {
        return Ok(AvromaticValue::Record(value.to_any_object()))
    }
    let out = record_class.protect_send("new", Some(&[value.to_any_object()]))
        .map(|object| AvromaticValue::Record(object))
        .map_err(|_| bad_coercion(value, "record"));
    out
}

pub fn initialize() {
    Class::new("ModelDescriptor", None).define(|_| ());
}

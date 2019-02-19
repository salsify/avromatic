use avro_rs::{
    FullSchema,
    Schema,
    schema::{RecordField, SchemaKind, SchemaIter, SchemaRef, UnionRef},
    types::Value as AvroValue,
};
use crate::model::{AvromaticModel, ModelStorage, MODEL_STORAGE_WRAPPER};
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

    pub fn deserialize(&self, class: &Class, data: &[u8]) -> Result<AnyObject, Error> {
        let mut cursor = std::io::Cursor::new(data);
        // TODO: need to get writer schema
        let value = avro_rs::from_avro_datum(&self.schema, &mut cursor, None)?;
        self.avro_to_model(class, value)
    }

    fn avro_to_model(&self, class: &Class, value: AvroValue) -> Result<AnyObject, Error> {
        let attributes = self.descriptor.avro_to_attributes(value)?;
        let storage = ModelStorage { attributes };
        let mut model = class.allocate().to_any_object();
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

    fn avro_to_attributes(&self, value: AvroValue) -> Result<HashMap<String, AvromaticValue>, Error> {
        match value {
            AvroValue::Record(fields) => {
                let mut attributes = HashMap::new();
                fields.into_iter().map(|(key, value)| {
                    let descriptor = self.attributes.get(&key)
                        .unwrap();
                    let attribute = descriptor.avro_to_attribute(value)?;
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

    fn avro_to_attribute(&self, value: AvroValue) -> Result<AvromaticValue, Error> {
        self.type_descriptor.avro_to_attribute(value)
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
    Union(HashMap<UnionRef, usize>, Vec<TypeDescriptor>),
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
                let union_schema = schema.union_schema();
                let variants = union_schema
                    .variants()
                    .into_iter()
                    .map(TypeDescriptor::build)
                    .collect();
                TypeDescriptor::Union(union_schema.union_ref_map(), variants)
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
            TypeDescriptor::Union(_, variants) => coerce_union(value, variants),
            TypeDescriptor::Record(inner) => coerce_record(value, inner),
            _ => Err(AvromaticError::Fuck.into()),
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

    fn avro_to_attribute(&self, value: AvroValue) -> Result<AvromaticValue, Error> {
        let out = match self {
            TypeDescriptor::Null => AvromaticValue::Null,
            TypeDescriptor::String => {
                if let AvroValue::String(s) = value {
                    AvromaticValue::String(s.into())
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
                    AvromaticValue::String(rstring)
                } else {
                    unimplemented!()
                }
            },
            TypeDescriptor::Array(inner) => {
                if let AvroValue::Array(values) = value {
                    let attributes = values.into_iter()
                        .map(|v| inner.avro_to_attribute(v))
                        .collect::<Result<Vec<AvromaticValue>, Error>>()?;
                    AvromaticValue::Array(attributes)
                } else {
                    unimplemented!()
                }
            },
            TypeDescriptor::Union(ref_index, schemas) => {
                if let AvroValue::Union(union_ref, value) = value {
                    if let Some(index) = ref_index.get(&union_ref) {
                        let value = schemas[*index].avro_to_attribute(*value)?;
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
                AvromaticValue::Record(descriptor.avro_to_model(inner, value)?)
            },
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
    record_class.protect_send("new", Some(&[value.to_any_object()]))
        .map(|object| AvromaticValue::Record(object))
        .map_err(|_| bad_coercion(value, "record"))
}

pub fn initialize() {
    Class::new("ModelDescriptor", None).define(|_| ());
}

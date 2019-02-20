use avro_rs::{FullSchema, Schema, UnionRef, schema::{SchemaIter, SchemaKind}, types::{Record, Value, ToAvro}};
use crate::model::MODEL_STORAGE_WRAPPER;
use crate::values::AvromaticValue;
use failure::{Error, bail};
use rutie::{Object, RString};
use std::collections::HashMap;

pub fn to_avro<'a, I>(
    schema: I,
    value: &AvromaticValue,
) -> Result<Value, Error>
    where I: SchemaIter<'a> + 'a
{
    let out = match (value, schema.kind()) {
        (AvromaticValue::Null, SchemaKind::Null) => Value::Null,
        (AvromaticValue::String(rstring), SchemaKind::Fixed) => {
            Value::Fixed(schema.fixed_size(), rstring.to_vec_u8_unchecked())
        },
        (AvromaticValue::String(rstring), SchemaKind::String) => Value::String(rstring.to_string()),
        (AvromaticValue::Long(integer), SchemaKind::Int) => Value::Long(integer.to_i64()),
        (AvromaticValue::Long(integer), SchemaKind::Long) => Value::Long(integer.to_i64()),
        (AvromaticValue::Union(n, ref value), SchemaKind::Union) => {
            let schema = schema.union_schema().variants()[*n];
            let union_ref = UnionRef::from_schema(schema.schema());
            Value::Union(union_ref, Box::new(to_avro(schema, &value)?))
        },
        (value, SchemaKind::Union) => {
            schema.union_schema()
                .variants()
                .into_iter()
                .map(|variant| to_avro(variant, value))
                .find(Result::is_ok)
                .unwrap_or_else(|| bail!("Bad union"))?
        },
        (AvromaticValue::Array(values), SchemaKind::Array) => Value::Array(
            values.into_iter()
                .map(|v| to_avro(schema.array_schema(), &v))
                .collect::<Result<Vec<Value>, Error>>()?
        ),
        (AvromaticValue::Record(object), SchemaKind::Record) => {
            let mut attributes = object.instance_variable_get("@_attributes");
            let storage = attributes.get_data_mut(&*MODEL_STORAGE_WRAPPER);
            build_avro_record(&storage.attributes, schema)?
        },
        _ => bail!("bad to avro: {:?} {:?}", value, schema.schema()),
    };
    Ok(out)
}

fn build_avro_record<'a, I>(
    attributes: &HashMap<String, AvromaticValue>,
    schema: I
) -> Result<Value, Error>
    where I: SchemaIter<'a> + 'a
{
    let schema = schema.record_schema();
    let mut record = schema.new_record();
    schema.fields().iter().map(|field| {
        let value = attributes.get(field.name()).unwrap_or(&AvromaticValue::Null);
        record.put(field.name(), to_avro(field.schema(), value)?);
        Ok(())
    }).collect::<Result<(), Error>>()?;
    Ok(record.avro())
}

pub fn serialize(
    schema: &FullSchema,
    attributes: &HashMap<String, AvromaticValue>,
) -> Result<Vec<u8>, Error> {
    let record = build_avro_record(attributes, schema)?;
    avro_rs::to_avro_datum(schema, record)
}

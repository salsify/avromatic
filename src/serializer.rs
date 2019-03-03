use avro_rs::{FullSchema, UnionRef, schema::{SchemaIter, SchemaKind}, types::{Value, ToAvro}};
use crate::model::MODEL_STORAGE_WRAPPER;
use crate::values::AvromaticValue;
use failure::{Error, bail, format_err};
use rutie::*;
use std::collections::HashMap;

pub fn to_avro<'a, I>(
    schema: I,
    value: &AvromaticValue,
) -> Result<Value, Error>
    where I: SchemaIter<'a> + 'a
{
    let out = match (value, schema.kind()) {
        (AvromaticValue::Null, SchemaKind::Null) => Value::Null,
        (AvromaticValue::String(rstring), SchemaKind::Fixed) =>
            fixed_to_value(rstring, schema.fixed_size().unwrap()),
        (AvromaticValue::String(rstring), SchemaKind::String) => string_to_value(rstring),
        (AvromaticValue::Long(integer), SchemaKind::Int) => int_to_value(integer),
        (AvromaticValue::Long(integer), SchemaKind::Long) => long_to_value(integer),
        (AvromaticValue::Long(integer), SchemaKind::Date) => date_to_value(integer),
        (AvromaticValue::Long(integer), SchemaKind::TimestampMillis) => timestamp_millis_to_value(integer),
        (AvromaticValue::Long(integer), SchemaKind::TimestampMicros) => timestamp_micros_to_value(integer),
        (AvromaticValue::Union(n, ref value), SchemaKind::Union) => union_to_value(*n, value, schema)?,
        (value, SchemaKind::Union) => untracked_union_to_value(value, schema)?,
        (AvromaticValue::Array(values), SchemaKind::Array) => array_to_value(values, schema)?,
        (AvromaticValue::Record(object), SchemaKind::Record) => record_to_value(object, schema)?,
        _ => return Err(format_err!("bad to avro: {:?} {:?}", value, schema.schema())),
    };
    Ok(out)
}

fn string_to_value(rstring: &RString) -> Value {
    Value::String(rstring.to_string())
}

fn fixed_to_value(rstring: &RString, size: usize) -> Value {
    Value::Fixed(size, rstring.to_vec_u8_unchecked())
}

fn int_to_value(integer: &Integer) -> Value {
    Value::Int(integer.to_i64() as i32)
}

fn long_to_value(integer: &Integer) -> Value {
    Value::Long(integer.to_i64())
}

fn date_to_value(integer: &Integer) -> Value {
    Value::Date(integer.to_i64() as i32)
}

fn timestamp_millis_to_value(integer: &Integer) -> Value {
    Value::TimestampMillis(integer.to_i64())
}

fn timestamp_micros_to_value(integer: &Integer) -> Value {
    Value::TimestampMicros(integer.to_i64())
}

fn union_to_value<'a, I>(index: usize, value: &AvromaticValue, schema: I) -> Result<Value, Error>
    where I: SchemaIter<'a> + 'a
{
    let schema = schema.union_schema().unwrap().variants()[index];
    let union_ref = UnionRef::from_schema(schema.schema());
    Ok(Value::Union(union_ref, Box::new(to_avro(schema, &value)?)))
}

fn untracked_union_to_value<'a, I>(value: &AvromaticValue, schema: I) -> Result<Value, Error>
    where I: SchemaIter<'a> + 'a
{
    schema.union_schema()
        .unwrap()
        .variants()
        .into_iter()
        .map(|variant| {
            let val = Box::new(to_avro(variant, value)?);
            Ok(Value::Union(UnionRef::from_schema(variant.schema()), val))
        })
        .find(Result::is_ok)
        .unwrap_or_else(|| bail!("Bad union"))
}


fn array_to_value<'a, I>(values: &[AvromaticValue], schema: I) -> Result<Value, Error>
    where I: SchemaIter<'a> + 'a
{
    Ok(
        Value::Array(
        values.into_iter()
            .map(|v| to_avro(schema.array_schema().unwrap(), &v))
            .collect::<Result<Vec<Value>, Error>>()?
        )
    )
}


fn record_to_value<'a, I>(object: &AnyObject, schema: I) -> Result<Value, Error>
    where I: SchemaIter<'a> + 'a
{
    let mut attributes = object.instance_variable_get("@_attributes");
    let storage = attributes.get_data_mut(&*MODEL_STORAGE_WRAPPER);
    build_avro_record(&storage.attributes, schema)
}

fn build_avro_record<'a, I>(
    attributes: &HashMap<String, AvromaticValue>,
    schema: I
) -> Result<Value, Error>
    where I: SchemaIter<'a> + 'a
{
    let schema = schema.record_schema().unwrap();
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

use avro_rs::{FullSchema, Schema, schema::SchemaIter};
use crate::schema::RAvroSchema;
use failure::{Error, format_err};
use rutie::*;

ruby_class!(
    AvromaticConfiguration,
    "Avromatic::Model::Configuration",
    Module::from_existing("Avromatic")
        .get_nested_module("Model")
        .get_nested_class("Configuration")
);

impl AvromaticConfiguration {
    pub fn new(schema: &FullSchema) -> Result<Self, Error> {
        let schema_name = schema.fullname().ok_or_else(|| format_err!("invalid schema"))?;
        let mut args = Hash::new();
        args.store(Symbol::new("schema_name"), RString::new_utf8(&schema_name));
        let schema_string = serde_json::to_string_pretty(&schema.schema)?;
        let rb_schema = Module::from_existing("Avro")
            .get_nested_class("Schema")
            .send("parse", Some(&[RString::new_utf8(&schema_string).into()]));

        args.store(Symbol::new("schema"), rb_schema);
        let instance = Self::class().new_instance(Some(&[args.to_any_object()]));
        Ok(instance.value().into())
    }

    pub fn rb_key_schema(&self) -> Option<RAvroSchema> {
        self.instance_variable_get("@key_avro_schema")
            .try_convert_to()
            .ok()
    }

    pub fn rb_value_schema(&self) -> RAvroSchema {
        self.instance_variable_get("@avro_schema").try_convert_to().unwrap()
    }

    pub fn key_schema(&self) -> Result<Option<FullSchema>, Error> {
        if let Some(mut schema) = self.rb_key_schema() {
            return Ok(Some(schema.rust_schema()?));
        }
        Ok(None)
    }
    pub fn value_schema(&self) -> Result<FullSchema, Error> {
        self.rb_value_schema().rust_schema()
    }

    pub fn is_mutable(&self) -> bool {
        self.instance_variable_get("@mutable")
            .try_convert_to::<Boolean>()
            .map(|b| b.to_bool())
            .unwrap_or(false)
    }

    pub fn is_nested_model(&self) -> bool {
        let key_var = self.instance_variable_get("@key_avro_schema");
        let value_var = self.instance_variable_get("@avro_schema");
        key_var.is_nil() && !value_var.is_nil()
    }
}

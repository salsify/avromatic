use avro_rs::{FullSchema, Schema, schema::SchemaIter};
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
        let schema_rstring = RString::new_utf8(&schema_name);
        let mut args = Hash::new();
        args.store(Symbol::new("schema_name"), schema_rstring);
        let instance = Self::class().new_instance(Some(&[args.to_any_object()]));
        Ok(instance.value().into())
    }

    pub fn key_schema(&self) -> Result<Option<FullSchema>, Error> {
        let var = self.instance_variable_get("@key_avro_schema");
        if var.is_nil() {
            return Ok(None)
        }
        let s = RString::from(var.send("to_s", None).value());
        Ok(Some(Schema::parse_str(s.to_str())?))
    }
    pub fn value_schema(&self) -> Result<FullSchema, Error> {
        let s = RString::from(
            self.instance_variable_get("@avro_schema")
                .send("to_s", None)
                .value()
        );
        Ok(Schema::parse_str(s.to_str())?)
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

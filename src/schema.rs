use avro_rs::{FullSchema, Schema};
use failure::Error;
use rutie::*;

pub struct ModelSchema {
    pub schema: FullSchema,
}

wrappable_struct!(
    ModelSchema,
    ModelSchemaWrapper,
    MODEL_SCHEMA_WRAPPER
);

// hacking around the macro a bit
ruby_class!(@ RAvroSchema, "Avro::Schema", "None");

impl RAvroSchema {
    pub fn rust_schema(&mut self) -> Result<FullSchema, Error> {
        let var = self.instance_variable_get("@_rust_schema");
        if var.is_nil() {
            let s = RString::from(self.protect_public_send("to_s", &[]).unwrap().value());
            let rust_schema = Schema::parse_str(s.to_str())?;
            let model_schema = ModelSchema { schema: rust_schema.clone() };
            let avro_schema: AnyObject = Class::from_existing("AvroSchema")
                .wrap_data(model_schema, &*MODEL_SCHEMA_WRAPPER);
            self.instance_variable_set("@_rust_schema", avro_schema);
            return Ok(rust_schema);
        }
        let existing_schema = var.get_data(&*MODEL_SCHEMA_WRAPPER);
        // TODO
        Ok(existing_schema.schema.clone())
    }
}

impl VerifiedObject for RAvroSchema {
    fn is_correct_type<T: Object>(obj: &T) -> bool {
        // this is very vague, but SalsifyAvro is yet another shitty hack job
        // that makes this hard to do
        obj.respond_to("type_sym")
    }

    fn error_message() -> &'static str {
        concat!("Error converting to Schema")
    }
}

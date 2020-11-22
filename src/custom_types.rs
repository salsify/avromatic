use rutie::*;

ruby_class!(
    CustomTypeConfiguration,
    "Avromatic::Model::CustomTypeConfiguration",
    Module::from_existing("Avromatic")
        .get_nested_module("Model")
        .get_nested_class("CustomTypeConfiguration")
);

impl CustomTypeConfiguration {

    pub fn serialize(&self, value: AnyObject) -> AnyObject {
        let to_avro = self.instance_variable_get("@to_avro");
        if !to_avro.is_nil() {
            return Self::call(to_avro, value);
        }

        value
    }

    pub fn deserialize(&self, value: AnyObject) -> AnyObject {
        let from_avro = self.instance_variable_get("@from_avro");
        if !from_avro.is_nil() {
            return Self::call(from_avro, value);
        }

        value
    }

    fn call(proc: AnyObject, value: AnyObject) -> AnyObject {
        if value.is_nil() {
            return value;
        }

        let proc = argument_check!(proc.try_convert_to::<Proc>());
        proc.call(&[value])
    }
}

ruby_class!(
    CustomTypeRegistry,
    "Avromatic::Model::CustomTypeRegistry",
    Module::from_existing("Avromatic")
        .get_nested_module("Model")
        .get_nested_class("CustomTypeRegistry")
);

impl CustomTypeRegistry {
    pub fn global() -> Self {
        Module::from_existing("Avromatic")
            .instance_variable_get("@custom_type_registry")
            .try_convert_to()
            .unwrap()
    }

    fn custom_types(&self) -> Hash {
        self.instance_variable_get("@custom_types").try_convert_to().unwrap()
    }

    pub fn fetch(&self, name: &str) -> Option<CustomTypeConfiguration> {
        self.custom_types()
            .at(&RString::new_utf8(name))
            .try_convert_to()
            .ok()
    }
}

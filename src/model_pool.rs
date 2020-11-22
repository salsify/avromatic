use rutie::*;

ruby_class!(
    ModelRegistry,
    "Avromatic::ModelRegistry",
    Module::from_existing("Avromatic").get_nested_class("ModelRegistry")
);

impl ModelRegistry {
    pub fn global() -> Self {
        Module::from_existing("Avromatic")
            .instance_variable_get("@nested_models")
            .try_convert_to()
            .unwrap()
    }

    pub fn lookup(&self, name: &str) -> Option<Class> {
        if !self.is_registered(name) {
            return None;
        }

        let obj = self.protect_public_send("[]", &[self.registry_key(name)])
            .expect("unexpected exception");
        return obj.try_convert_to::<Class>().ok()
    }

    pub fn is_registered(&self, name: &str) -> bool {
        self.protect_public_send("registered?", &[self.registry_key(name)])
            .expect("unexpected exception")
            .try_convert_to::<Boolean>()
            .map(|b| b.to_bool())
            .unwrap_or(false)
    }

    pub fn register(&self, class: &Class) {
        self.protect_public_send("register", &[class.to_any_object()]).unwrap();
    }

    fn registry_key(&self, string: &str) -> AnyObject {
        let prefix = self.instance_variable_get("@prefix");
        if prefix.is_nil() {
            return RString::new_utf8(string).into();
        }
        let prefix = prefix.try_convert_to::<RString>().unwrap();
        let prefix = prefix.to_str();
        let s = if string.starts_with(prefix) {
            if string.get(prefix.len() .. prefix.len() + 1) == Some(".") {
                &string[prefix.len() + 1 ..]
            } else {
                &string[prefix.len()..]
            }
        } else {
            string
        };
        RString::new_utf8(s).into()
    }
}

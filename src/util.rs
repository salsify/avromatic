use rutie::*;

pub fn instance_of(object: &impl Object, class: Class) -> bool {
    object.class().ancestors().iter().any(|obj_class| *obj_class == class)
}

// TODO: stupid hacks
pub fn ancestor_send(object: &impl Object, method: &str) -> AnyObject {
    class_ancestor_send(&object.class(), method)
}

pub fn class_ancestor_send(class: &Class, method: &str) -> AnyObject {
    for class in class.ancestors().iter() {
        let value = class.protect_public_send(method, &[]).unwrap();
        if !value.is_nil() {
            return value;
        }
    }
    return NilClass::new().into();
}

pub fn debug_ruby(object: &impl Object) -> String {
    format!("{}", object.protect_public_send("inspect", &[])
        .expect("unexpected exception")
        .try_convert_to::<RString>().unwrap().to_string())
}

ruby_class!(RDate, "Date");

impl RDate {
    pub fn from_i64(n: i64) -> AnyObject {
        Self::from_integer(&Integer::new(n))
    }

    pub fn from_integer(n: &Integer) -> AnyObject {
        let epoch = Module::from_existing("Avro")
            .get_nested_module("LogicalTypes")
            .get_nested_module("IntDate")
            .const_get("EPOCH_START");
        epoch.protect_public_send("+", &[n.to_any_object()]).unwrap()
    }

    pub fn days_since_epoch(&self) -> Integer {
        let epoch = Module::from_existing("Avro")
            .get_nested_module("LogicalTypes")
            .get_nested_module("IntDate")
            .const_get("EPOCH_START");
        self.protect_public_send("-", &[epoch])
            .unwrap()
            .protect_public_send("to_i", &[])
            .unwrap()
            .try_convert_to::<Integer>()
            .unwrap()
    }
}

ruby_class!(RDateTime, "DateTime");

impl RDateTime {
    pub fn days_since_epoch(&self) -> Integer {
        self.protect_public_send("to_i", &[])
            .unwrap()
            .try_convert_to::<Integer>()
            .map(|i| Integer::new(i.to_i64() / 60 / 60 / 24))
            .unwrap()
    }
}

ruby_class!(RTime, "Time");

impl RTime {
    pub fn days_since_epoch(&self) -> Integer {
        self.protect_public_send("to_i", &[])
            .unwrap()
            .try_convert_to::<Integer>()
            .map(|i| Integer::new(i.to_i64() / 60 / 60 / 24))
            .unwrap()
    }

    pub fn to_millis(&self) -> Integer {
        let seconds = self.protect_public_send("to_i", &[]).unwrap().try_convert_to::<Integer>().unwrap().to_i64();
        let micros = self.protect_public_send("usec", &[]).unwrap().try_convert_to::<Integer>().unwrap().to_i64();
        (seconds * 1000 + micros / 1000).into()
    }

    pub fn to_micros(&self) -> Integer {
        let seconds = self.protect_public_send("to_i", &[]).unwrap().try_convert_to::<Integer>().unwrap().to_i64();
        let micros = self.protect_public_send("usec", &[]).unwrap().try_convert_to::<Integer>().unwrap().to_i64();
        (seconds * 1000000 + micros).into()
    }

    pub fn from_i64_millis(n: i64) -> AnyObject {
        let seconds = Integer::new(n / 1000).to_any_object();
        let micros = Integer::new(n % 1000 * 1000).to_any_object();
        Class::from_existing("Time")
            .protect_public_send("at", &[seconds, micros]).unwrap()
            .protect_public_send("utc", &[]).unwrap()
    }

    pub fn from_millis(n: &Integer) -> AnyObject {
        Self::from_i64_millis(n.to_i64())
    }

    pub fn from_i64_micros(n: i64) -> AnyObject {
        let seconds = Integer::new(n / 1000000).to_any_object();
        let micros = Integer::new(n % 1000000).to_any_object();
        Class::from_existing("Time")
            .protect_public_send("at", &[seconds, micros]).unwrap()
            .protect_public_send("utc", &[]).unwrap()
    }

    pub fn from_micros(n: &Integer) -> AnyObject {
        Self::from_i64_micros(n.to_i64())
    }

}

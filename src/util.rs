use rutie::*;
use rutie::types::Value;
use std::borrow::Borrow;
use std::ops::Deref;

pub fn instance_of(object: &impl Object, class: Class) -> bool {
    object.class().ancestors().iter().any(|obj_class| *obj_class == class)
}

ruby_class!("Date", RDate);

impl RDate {
    pub fn from_i64(n: &Integer) -> AnyObject {
        let epoch = Module::from_existing("Avro")
            .get_nested_module("LogicalTypes")
            .get_nested_module("IntDate")
            .const_get("EPOCH_START");
        epoch.send("+", Some(&[n.to_any_object()]))
    }

    pub fn days_since_epoch(&self) -> Integer {
        let epoch = Module::from_existing("Avro")
            .get_nested_module("LogicalTypes")
            .get_nested_module("IntDate")
            .const_get("EPOCH_START");
        self.send("-", Some(&[epoch])).send("to_i", None)
            .try_convert_to::<Integer>()
            .unwrap()
    }
}

ruby_class!("DateTime", RDateTime);

impl RDateTime {
    pub fn days_since_epoch(&self) -> Integer {
        self.send("to_i", None)
            .try_convert_to::<Integer>()
            .map(|i| Integer::new(i.to_i64() / 60 / 60 / 24))
            .unwrap()
    }
}

ruby_class!("Time", RTime);

impl RTime {
    pub fn days_since_epoch(&self) -> Integer {
        self.send("to_i", None)
            .try_convert_to::<Integer>()
            .map(|i| Integer::new(i.to_i64() / 60 / 60 / 24))
            .unwrap()
    }

    pub fn to_millis(&self) -> Integer {
        let seconds = self.send("to_i", None).try_convert_to::<Integer>().unwrap().to_i64();
        let micros = self.send("usec", None).try_convert_to::<Integer>().unwrap().to_i64();
        (seconds * 1000 + micros / 1000).into()
    }

    pub fn to_micros(&self) -> Integer {
        let seconds = self.send("to_i", None).try_convert_to::<Integer>().unwrap().to_i64();
        let micros = self.send("usec", None).try_convert_to::<Integer>().unwrap().to_i64();
        (seconds * 1000000 + micros).into()
    }

    pub fn from_millis(n: &Integer) -> AnyObject {
        let seconds = Integer::new(n.to_i64() / 1000).to_any_object();
        let micros = Integer::new(n.to_i64() % 1000 * 1000).to_any_object();
        Class::from_existing("Time")
            .send("at", Some(&[seconds, micros]))
            .send("utc", None)
    }

    pub fn from_micros(n: &Integer) -> AnyObject {
        let seconds = Integer::new(n.to_i64() / 1000000).to_any_object();
        let micros = Integer::new(n.to_i64() % 1000000).to_any_object();
        Class::from_existing("Time")
            .send("at", Some(&[seconds, micros]))
            .send("utc", None)
    }
}

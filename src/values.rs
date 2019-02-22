use rutie::*;
use rutie::rubysys::value::ValueType;
use std::collections::HashMap;

#[derive(Debug)]
pub enum AvromaticValue {
    Null,
    True,
    False,
    String(RString),
    Long(Integer),
    Float(Float),
    Array(Vec<AvromaticValue>),
    Map(HashMap<String, AvromaticValue>),
    Union(usize, Box<AvromaticValue>),
    Record(AnyObject),
}

impl AvromaticValue {
    pub fn to_any_object(&self) -> AnyObject {
        // TODO probably shouldn't convert arrays / hashes
        match self {
            AvromaticValue::Null => NilClass::new().to_any_object(),
            AvromaticValue::True => Boolean::new(true).to_any_object(),
            AvromaticValue::False => Boolean::new(false).to_any_object(),
            AvromaticValue::String(string) => string.to_any_object(),
            AvromaticValue::Long(n) => n.to_any_object(),
            AvromaticValue::Float(f) => f.to_any_object(),
            AvromaticValue::Array(values) =>
                values.iter().map(|v| v.to_any_object()).collect::<Array>().to_any_object(),
            AvromaticValue::Union(_, value) => value.to_any_object(),
            AvromaticValue::Record(value) => value.to_any_object(),
            AvromaticValue::Map(value) => {
                let mut hash = Hash::new();
                value.iter().for_each(|(k, v)| {
                    hash.store(RString::new_utf8(k), v.to_any_object());
                });
                hash.to_any_object()
            }
        }
    }

    pub fn mark(&self) {
        match self {
            AvromaticValue::Null | AvromaticValue::True | AvromaticValue::False => (),
            AvromaticValue::String(string) => GC::mark(string),
            AvromaticValue::Long(n) => GC::mark(n),
            AvromaticValue::Float(f) => GC::mark(f),
            AvromaticValue::Array(values) => values.iter().for_each(Self::mark),
            AvromaticValue::Union(_, value) => value.mark(),
            AvromaticValue::Record(value) => GC::mark(value),
            AvromaticValue::Map(map) => map.values().for_each(Self::mark),
        }
    }
}

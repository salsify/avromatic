use rutie::*;
use std::collections::HashMap;

#[derive(Debug, PartialEq)]
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
    Custom(AnyObject),
}

impl AvromaticValue {
    pub fn mark(&self) {
        match self {
            AvromaticValue::Null | AvromaticValue::True | AvromaticValue::False => (),
            AvromaticValue::String(string) => GC::mark(string),
            AvromaticValue::Long(n) => GC::mark(n),
            AvromaticValue::Float(f) => GC::mark(f),
            AvromaticValue::Array(values) => values.iter().for_each(Self::mark),
            AvromaticValue::Union(_, value) => value.mark(),
            AvromaticValue::Record(value) => GC::mark(value),
            AvromaticValue::Custom(value) => GC::mark(value),
            AvromaticValue::Map(map) => map.values().for_each(Self::mark),
        }
    }
}

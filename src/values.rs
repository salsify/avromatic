use rutie::*;
use rutie::rubysys::value::ValueType;

#[derive(Debug)]
pub enum AvromaticValue {
    Null,
    True,
    False,
    String(RString),
    Long(Integer),
    Float(Float),
    Array(Vec<AvromaticValue>),
    Union(usize, Box<AvromaticValue>),
    Record(AnyObject),
}

impl AvromaticValue {
    pub fn to_any_object(&self) -> AnyObject {
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
        }
    }
}

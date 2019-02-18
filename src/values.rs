use rutie::*;
use rutie::rubysys::value::ValueType;

#[derive(Debug)]
pub enum AvromaticValue {
    Null,
    String(RString),
    Long(Integer),
    Array(Vec<AvromaticValue>),
    Union(usize, Box<AvromaticValue>),
    Record(AnyObject),
}

impl From<RString> for AvromaticValue {
    fn from(rstring: RString) -> Self {
        AvromaticValue::String(rstring)
    }
}

impl From<Integer> for AvromaticValue {
    fn from(int: Integer) -> Self {
        AvromaticValue::Long(int)
    }
}

impl AvromaticValue {
    pub fn from_any_object(value: AnyObject) -> Option<Self> {
        unsafe {
            match value.ty() {
                ValueType::RString => Some(value.to::<RString>().into()),
                ValueType::Fixnum => Some(value.to::<Integer>().into()),
                _ => None,
            }
        }
    }

    pub fn to_any_object(&self) -> AnyObject {
        match self {
            AvromaticValue::Null => NilClass::new().to_any_object(),
            AvromaticValue::String(string) => string.to_any_object(),
            AvromaticValue::Long(n) => n.to_any_object(),
            AvromaticValue::Array(values) =>
                values.iter().map(|v| v.to_any_object()).collect::<Array>().to_any_object(),
            AvromaticValue::Union(_, value) => value.to_any_object(),
            AvromaticValue::Record(value) => value.to_any_object(),
        }
    }

    pub fn mark(&self) {
        match self {
            AvromaticValue::Null => (),
            AvromaticValue::String(string) => GC::mark(string),
            AvromaticValue::Long(n) => GC::mark(n),
            AvromaticValue::Array(values) => values.iter().for_each(Self::mark),
            AvromaticValue::Union(_, value) => value.mark(),
            AvromaticValue::Record(value) => GC::mark(value),
        }
    }
}

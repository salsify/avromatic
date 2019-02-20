use crate::values::AvromaticValue;
use rutie::*;
use rutie::types::Value;
use std::ops::Drop;

pub struct HeapGuard {
    values: Array
}

impl HeapGuard {
    pub fn new() -> Self {
        Self {
            values: Array::new(),
        }
    }

    pub fn guard<O: Object>(&mut self, v: O) {
        self.values.push(v);
    }

    pub fn guard_value(&mut self, value: &AvromaticValue) {
        self.guard(value.to_any_object())
        // match value {
        //     AvromaticValue::String(s) => self.guard(s),
        // }
    }
}

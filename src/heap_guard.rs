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
}

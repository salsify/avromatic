use rutie::*;

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

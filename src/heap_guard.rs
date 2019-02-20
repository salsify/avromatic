use rutie::*;
use rutie::types::Value;
use std::ops::Drop;

#[derive(Default)]
pub struct HeapGuard {
    values: Vec<AnyObject>
}

impl HeapGuard {
    pub fn guard(&mut self, v: &impl Object) {
        GC::register(v);
        let value = v.to_any_object();
        self.values.push(value);
    }
}

impl Drop for HeapGuard {
    fn drop(&mut self) {
        self.values.iter().for_each(GC::unregister)
    }
}

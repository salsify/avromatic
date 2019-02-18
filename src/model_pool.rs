use rutie::*;
use std::collections::HashMap;

#[derive(Debug)]
pub struct ModelPool {
    models: HashMap<String, Class>
}

impl ModelPool {
    pub fn new() -> Self {
        Self {
            models: HashMap::new(),
        }
    }

    pub fn register(&mut self, name: String, class: Class) {
        self.models.insert(name, class);
    }

    pub fn lookup(&self, name: &str) -> Option<Class> {
        self.models.get(name).map(|class| class.value().into())
    }

    pub fn mark(&self) {
        self.models.values().for_each(GC::mark);
    }
}

wrappable_struct!(
    ModelPool,
    ModelPoolWrapper,
    MODEL_POOL_WRAPPER,
    mark(data) {
        data.mark();
    }
);

class!(ModelRegistry);

impl ModelRegistry {
    pub fn new() -> AnyObject {
        let pool = ModelPool::new();
        Class::from_existing("ModelRegistry").wrap_data(pool, &*MODEL_POOL_WRAPPER)
    }

    pub fn get(obj: &mut AnyObject) -> &mut ModelPool {
        obj.get_data_mut(&*MODEL_POOL_WRAPPER)
    }
}

methods!(
    ModelRegistry,
    _itself,

    fn rb_global() -> AnyObject {
        let mut registry = _itself.instance_variable_get("@_registry");
        if registry.is_nil() {
            return _itself.instance_variable_set("@_registry", ModelRegistry::new());
        }
        registry
    }
);

pub fn initialize() {
    Class::new("ModelRegistry", None).define(|itself| {
        itself.def_self("global", rb_global);
    });
}

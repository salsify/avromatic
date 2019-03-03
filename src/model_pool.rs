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

methods!(
    ModelRegistry,
    itself,

    fn rb_lookup(name: RString) -> AnyObject {
        let name = argument_check!(name);
        let pool = itself.get_data(&*MODEL_POOL_WRAPPER);
        pool.lookup(name.to_str())
            .as_ref()
            .map(Class::to_any_object)
            .unwrap_or_else(|| NilClass::new().to_any_object())
    }
);

impl ModelRegistry {
    pub fn new() -> AnyObject {
        let pool = ModelPool::new();
        Class::from_existing("ModelRegistry").wrap_data(pool, &*MODEL_POOL_WRAPPER)
    }

    pub fn get(obj: &mut AnyObject) -> &mut ModelPool {
        obj.get_data_mut(&*MODEL_POOL_WRAPPER)
    }

    pub fn register(name: String, class: Class) {
        println!("Registering: {}", name);
        let mut registry_obj = Self::global();
        let registry = Self::get(&mut registry_obj);
        registry.register(name, class)
    }

    pub fn lookup(name: &str) -> Option<Class> {
        let mut registry_obj = Self::global();
        let registry = Self::get(&mut registry_obj);
        registry.lookup(name)
    }

    pub fn global() -> AnyObject {
        let mut itself = Class::from_existing("ModelRegistry");
        let mut registry = itself.instance_variable_get("@_registry");
        if registry.is_nil() {
            return itself.instance_variable_set("@_registry", ModelRegistry::new());
        }
        registry
    }
}

methods!(
    ModelRegistry,
    itself,

    fn rb_global() -> AnyObject {
        ModelRegistry::global()
    }
);

pub fn initialize() {
    Class::new("ModelRegistry", None).define(|itself| {
        itself.def_self("global", rb_global);
        itself.def("[]", rb_lookup);
    });
}

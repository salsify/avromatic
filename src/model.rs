use avro_rs::{FullSchema, Schema, schema::SchemaIter};
use crate::heap_guard::HeapGuard;
use crate::descriptors::{ModelDescriptor, MODEL_DESCRIPTOR_WRAPPER};
use crate::model_pool::{ModelPool, ModelRegistry};
use crate::values::AvromaticValue;
use rutie::*;
use rutie::types::{Argc, Value as RValue};
use std::collections::HashMap;

#[derive(Default)]
pub struct ModelStorage {
    pub attributes: HashMap<String, AvromaticValue>,
}

wrappable_struct!(
    ModelStorage,
    ModelStorageWrapper,
    MODEL_STORAGE_WRAPPER,
    mark(data) {
        data.attributes.iter().for_each(|(_, v)| v.mark());
    }
);

module!(AvromaticModelAttributes);

fn stringify(object: AnyObject) -> String {
    object.try_convert_to::<RString>().unwrap().to_string()
}

extern fn rb_initialize(argc: Argc, argv: *const AnyObject, mut itself: AnyObject) -> AnyObject {
    // rutie doesn't support optional arguments yet, so we'll do it manually...
    let arg = RValue::from(0);
    unsafe {
        let p_argv: *const RValue = std::mem::transmute(argv);
        rutie::rubysys::class::rb_scan_args(
            argc,
            p_argv,
            rutie::util::str_to_cstring("01").as_ptr(),
            &arg
        );
    };
    let arg_obj: AnyObject = arg.into();
    let data: HashMap<String, AnyObject> = if arg.is_nil() {
        HashMap::new()
    } else {
        let hash = argument_check!(arg_obj.try_convert_to::<Hash>());
        let mut values = HashMap::new();
        hash.each(|key, value| {
            if let Ok(rstring) = key.try_convert_to::<RString>() {
                values.insert(rstring.to_string(), value);
            } else if let Ok(symbol) = key.try_convert_to::<Symbol>() {
                values.insert(symbol.to_string(), value);
            }
        });
        values
    };
    let object = itself.class().send("_schema", None);
    let descriptor = object.get_data(&*MODEL_DESCRIPTOR_WRAPPER);
    let mut storage = ModelStorage::default();
    let mut guard = HeapGuard::new();
    descriptor.each_field(|key, attribute| {
        let v = data.get(key).map(Object::to_any_object).unwrap_or(attribute.default());
        match attribute.coerce(v, &mut guard) {
            Ok(coerced) => {
                 storage.attributes.insert(key.to_string(), coerced);
            },
            Err(err) => {
                let message = format!("error initializing {}: {}", key, err);
                VM::raise(Class::from_existing("ArgumentError"), &message);
            },
        };
    });
    let wrapped_storage: AnyObject = itself.class().wrap_data(storage, &*MODEL_STORAGE_WRAPPER);
    itself.instance_variable_set("@_attributes", wrapped_storage);
    NilClass::new().into()
}

methods!(
    AvromaticModelAttributes,
    itself,

    fn rb_set_attribute(key: RString, value: AnyObject) -> AnyObject {
        let key = argument_check!(key);
        let key = key.to_str();
        let value = argument_check!(value);
        let mut guard = HeapGuard::new();
        let avromatic_value_result = itself.class().send("_schema", None)
            .get_data(&*MODEL_DESCRIPTOR_WRAPPER)
            .coerce(key, value, &mut guard);
        if let Err(err) = avromatic_value_result {
            let message = format!("{}", err);
            VM::raise(Class::from_existing("ArgumentError"), &message);
            return NilClass::new().into();
        }

        let value = avromatic_value_result.unwrap();
        itself.with_storage(|storage| {
            storage
                .attributes
                .insert(key.to_string(), value.into());
        });
        NilClass::new().into()
    }

    fn rb_get_attribute(key: AnyObject) -> AnyObject {
        let key = stringify(argument_check!(key));
        itself.with_storage(|storage| {
            if let Some(value) = storage.attributes.get(&key) {
                value.to_any_object()
            } else {
                NilClass::new().into()
            }
        })
    }

    fn rb_is_attribute_true(key: AnyObject) -> AnyObject {
        let key = stringify(argument_check!(key));
        itself.with_storage(|storage| {
            if let Some(AvromaticValue::True) = storage.attributes.get(&key) {
                Boolean::new(true)
            } else {
                Boolean::new(false)
            }
        }).to_any_object()
    }

    fn rb_avro_message_value() -> AnyObject {
        let encoding = Encoding::find("ASCII-8BIT").unwrap();
        let bytes = itself.serialize();
        let rstring = RString::from_bytes(&bytes, &encoding);
        rstring.into()
    }

    fn rb_avro_message_decode(data: RString) -> AnyObject {
        let data = argument_check!(data);
        let schema = itself.send("_schema", None);
        let descriptor = schema.get_data(&*MODEL_DESCRIPTOR_WRAPPER);
        let mut guard = HeapGuard::new();
        descriptor.deserialize(
            &Class::from(itself.value()),
            &data.to_bytes_unchecked(),
            &mut guard
        ).unwrap()
    }
);

impl AvromaticModelAttributes {
    fn with_storage<F, R>(&self, f: F) -> R
        where F: FnOnce(&mut ModelStorage) -> R
    {
        let mut attributes = self.instance_variable_get("@_attributes");
        let storage = attributes.get_data_mut(&*MODEL_STORAGE_WRAPPER);
        f(storage)
    }

    fn serialize(&self) -> Vec<u8> {
        let schema = self.class().send("_schema", None);
        let descriptor = schema.get_data(&*MODEL_DESCRIPTOR_WRAPPER);
        self.with_storage(|storage| descriptor.serialize(&storage.attributes).unwrap())
    }
}

module!(AvromaticModel);

methods!(
    AvromaticModel,
    _itself,

    fn rb_included_hook(model_class: Class) -> AnyObject {
        let mut model_class = argument_check!(model_class);
        model_class.include("AvromaticModelAttributes");

        let descriptor = _itself.instance_variable_get("@_schema");
        model_class.instance_variable_set("@_schema", descriptor);
        model_class.singleton_class().attr_reader("_schema");

        model_class.def_self("avro_message_decode", rb_avro_message_decode);

        NilClass::new().into()
    }

    fn rb_build(schema_str: RString) -> AnyObject {
        let schema_str = argument_check!(schema_str).to_string();
        let schema = Schema::parse_str(&schema_str).unwrap();
        let module = AvromaticModel::from_schema(schema);
        module.into()
    }
);

impl AvromaticModel {
    pub fn from_schema(schema: FullSchema) -> Module {
        let mut module = Class::from_existing("Module")
            .send("new", None)
            .try_convert_to::<Module>()
            .unwrap();

        match ModelDescriptor::new(schema) {
            Ok(descriptor) => {
                module.instance_variable_set("@_schema", descriptor);
                module.define(|itself| {
                    itself.def_self("included", rb_included_hook);
                });
            },
            Err(err) => VM::raise(Class::from_existing("StandardError"), &format!("{}", err)),
        }
        module
    }

    pub fn build_model(schema: FullSchema) -> Class {
        let mut registry_obj = Class::from_existing("ModelRegistry")
            .send("global", None);
        let registry = ModelRegistry::get(&mut registry_obj);

        let model_name = (&schema).fullname().unwrap();
        if let Some(class) = registry.lookup(&model_name) {
            return class;
        }
        println!("Allocating model: {:?}", model_name);
        let class = Class::new(&model_name, None);
        registry.register(model_name, class.value().into());
        let module = Self::from_schema(schema);
        class.send("include", Some(&[module.to_any_object()]));
        class
    }
}

pub fn initialize() {
    Module::new("AvromaticModelAttributes").define(|itself| {
        itself.def("initialize", rb_initialize);
        itself.def("[]=", rb_set_attribute);
        itself.def("[]", rb_get_attribute);
        itself.def("avro_message_value", rb_avro_message_value);
    });
    Module::new("AvromaticModel").define(|itself| {
        itself.def_self("build", rb_build);
    });
}

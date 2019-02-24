use avro_rs::{FullSchema, schema::{ SchemaIter, SchemaKind }};
use crate::configuration::AvromaticConfiguration;
use crate::heap_guard::HeapGuard;
use crate::descriptors::{ModelDescriptor, MODEL_DESCRIPTOR_WRAPPER};
use crate::model_pool::ModelRegistry;
use crate::values::AvromaticValue;
use failure::{Error, format_err};
use rutie::*;
use rutie::types::{Argc, Value as RValue};
use sha2::Sha256;
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

fn raise_if_error<T: Object>(result: Result<T, Error>) -> AnyObject {
    match result {
        Ok(t) => t.to_any_object(),
        Err(err) => {
            let message = format!("{}", err);
            VM::raise(Class::from_existing("StandardError"), &message);
            NilClass::new().to_any_object()
        }
    }
}

fn stringify(object: &AnyObject) -> String {
    object.try_convert_to::<RString>()
        .map(|s| s.to_string())
        .or_else(|_| object.try_convert_to::<Symbol>().map(|s| s.to_string()))
        .unwrap()
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
    let object = crate::util::ancestor_send(&itself, "_schema");
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
                let class = Module::from_existing("Avromatic")
                    .get_nested_module("Model")
                    .get_nested_class("CoercionError");
                let message = format!("error initializing {}: {}", key, err);
                VM::raise(class, &message);
            },
        };
    });
    let wrapped_storage: AnyObject = itself.class().wrap_data(storage, &*MODEL_STORAGE_WRAPPER);
    itself.instance_variable_set("@_attributes", wrapped_storage);
    NilClass::new().into()
}

extern fn rb_method_missing(argc: Argc, argv: *const AnyObject, itself: AvromaticModelAttributes) -> AnyObject {
    let name_arg = RValue::from(0);
    let args = RValue::from(0);

    unsafe {
        let p_argv: *const RValue = std::mem::transmute(argv);

        rutie::rubysys::class::rb_scan_args(
            argc,
            p_argv,
            rutie::util::str_to_cstring("1*").as_ptr(),
            &name_arg,
            &args
        )
    };

    let name_obj = AnyObject::from(name_arg);
    let arguments = Array::from(args);
    let name = argument_check!(name_obj.try_convert_to::<Symbol>());
    let s = name.to_str();

    if s.ends_with("?") {
        let (s, _) = s.split_at(s.len() - 1);
        let args: [AnyObject; 1] = [RString::new_utf8(s).to_any_object()];
        itself.send("_attribute_true?", Some(&args))
    } else if s.ends_with("=") {
        if itself.config().is_mutable() {
            let (s, _) = s.split_at(s.len() - 1);
            let args: [AnyObject; 2] = [RString::new_utf8(s).to_any_object(), arguments.at(1)];
            itself.send("[]=", Some(&args))
        } else {
            let message = format!(
                "private method `{}' called for {}",
                s,
                itself.class().send("to_s", None).try_convert_to::<RString>().map(|s| s.to_string())
                    .unwrap_or_else(|_| "Object".to_string())
            );
            VM::raise(Class::from_existing("NoMethodError"), &message);
            NilClass::new().into()
        }
    } else {
        let args: [AnyObject; 1] = [RString::new_utf8(s).to_any_object()];
        itself.send("[]", Some(&args))
    }
}

methods!(
    AvromaticModelAttributes,
    itself,

    fn rb_attributes() -> AnyObject {
        itself.attribute_hash().into()
    }

    fn rb_set_attribute(key: AnyObject, value: AnyObject) -> AnyObject {
        let key = argument_check!(key);
        let key = stringify(&key);
        let value = argument_check!(value);
        let mut guard = HeapGuard::new();
        let avromatic_value_result = crate::util::ancestor_send(&itself, "_schema")
            .get_data(&*MODEL_DESCRIPTOR_WRAPPER)
            .coerce(&key, value, &mut guard);
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
        let key = argument_check!(key);
        let key = stringify(&key);
        itself.with_storage(|storage| {
            if let Some(value) = storage.attributes.get(&key) {
                let schema = crate::util::ancestor_send(&itself, "_schema");
                let descriptor = schema.get_data(&*MODEL_DESCRIPTOR_WRAPPER);
                descriptor.to_ruby(&key, &value)
            } else {
                NilClass::new().into()
            }
        })
    }

    fn rb_is_attribute_true(key: AnyObject) -> AnyObject {
        let key = argument_check!(key);
        let key = stringify(&key);
        itself.with_storage(|storage| {
            let stored = storage.attributes.get(&key);
            if let Some(AvromaticValue::True) = stored {
                Boolean::new(true)
            } else if let Some(AvromaticValue::Union(1, ref boxed)) = stored {
                Boolean::new(**boxed == AvromaticValue::True)
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

    fn rb_attribute_definitions() -> Hash {
        let schema = itself.send("_schema", None);
        let descriptor = schema.get_data(&*MODEL_DESCRIPTOR_WRAPPER);
        let mut hash = Hash::new();
        descriptor.each_field(|k, _| {
            let key = Symbol::new(k);
            hash.store(key, NilClass::new());
        });
        hash
    }

    fn rb_respond_to_missing(name: Symbol, _include_all: Boolean) -> AnyObject {
        let name = argument_check!(name);
        let s = name.to_str();
        let schema = crate::util::ancestor_send(&itself, "_schema");
        let descriptor = schema.get_data(&*MODEL_DESCRIPTOR_WRAPPER);
        let b = if s.ends_with("?") {
            let (s, _) = s.split_at(s.len() - 1);
            descriptor.get_attribute(s)
                .map(|d| d.is_boolean())
                .unwrap_or(false)
        } else if s.ends_with("=") {
            if itself.config().is_mutable() {
                let (s, _) = s.split_at(s.len() - 1);
                descriptor.get_attribute(s).is_some()
            } else {
                false
            }
        } else {
            descriptor.get_attribute(s).is_some()
        };
        Boolean::new(b).to_any_object()
    }

    fn rb_clone() -> AnyObject {
        itself.to_any_object()
    }

    fn rb_hash() -> AnyObject {
        itself.attribute_hash().send("hash", None)
    }

    fn rb_to_s() -> RString {
        let s = format!(
            "#<{}:{}>",
            "Class",
            itself.send("object_id", None).try_convert_to::<Integer>().unwrap().to_i64(),
        );
        RString::new_utf8(&s)
    }

    fn rb_equal(other: AnyObject) -> AnyObject {
        let other = argument_check!(other);
        if itself.class() == other.class() {
            return Boolean::new(false).into();
        }

        let other = unsafe { other.to::<AvromaticModelAttributes>() };
        let other_hash = other.attribute_hash().to_any_object();
        itself.attribute_hash().send("==", Some(&[other_hash])).into()
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
        let schema = crate::util::ancestor_send(self, "_schema");
        let descriptor = schema.get_data(&*MODEL_DESCRIPTOR_WRAPPER);
        self.with_storage(|storage| descriptor.serialize(&storage.attributes).unwrap())
    }

    fn attribute_hash(&self) -> Hash {
        let schema = crate::util::ancestor_send(self, "_schema");
        let descriptor = schema.get_data(&*MODEL_DESCRIPTOR_WRAPPER);
        let mut hash = Hash::new();
        self.with_storage(|storage| {
            storage.attributes.iter().for_each(|(k, v)| {
                let key = Symbol::new(k);
                let value = descriptor.to_ruby(k, v);
                hash.store(key, value);
            });
        });
        hash
    }

    fn config(&self) -> AvromaticConfiguration {
        unsafe { self.instance_variable_get("@config").to() }
    }
}

module!(AvromaticModel);

methods!(
    AvromaticModel,
    _itself,

    fn rb_included_hook(model_class: Class) -> AnyObject {
        let mut model_class = argument_check!(model_class);

        let descriptor = _itself.instance_variable_get("@_schema");
        model_class.instance_variable_set("@_schema", descriptor.to_any_object());
        model_class.singleton_class().attr_reader("_schema");
        let inner = descriptor.get_data(&*MODEL_DESCRIPTOR_WRAPPER);

        let mut config = _itself.instance_variable_get("@config");
        if config.is_nil() {
            config = AvromaticConfiguration::new(inner.value_schema()).unwrap().to_any_object();
        }
        let config = config.try_convert_to::<AvromaticConfiguration>().unwrap();
        if config.is_nested_model() {
            let model_name = _itself.instance_variable_get("@name").try_convert_to::<RString>().unwrap().to_string();
            if let Some(ref class) = ModelRegistry::lookup(&model_name) {
                let schema = config.value_schema().unwrap();
                AvromaticModel::validate_fingerprints(&schema, &model_name, class);
            } else {
                ModelRegistry::register(model_name, model_class.value().into());
            }
        }

        model_class.instance_variable_set("@config", config);
        model_class.singleton_class().attr_reader("config");

        model_class.include("AvromaticModelAttributes");

        model_class.def_self("avro_message_decode", rb_avro_message_decode);
        model_class.def_self("attribute_definitions", rb_attribute_definitions);

        NilClass::new().into()
    }

    fn rb_build(config: AvromaticConfiguration) -> AnyObject {
        let config = argument_check!(config);
        raise_if_error(AvromaticModel::from_config(config).into())
    }
);

impl AvromaticModel {
    pub fn from_config(config: AvromaticConfiguration) -> Result<Module, Error> {
        let maybe_key_schema = config.key_schema()?;
        let value_schema = config.value_schema()?;
        let mut module = Self::from_schema(maybe_key_schema, value_schema)?;
        module.instance_variable_set("@config", config);
        Ok(module)
    }

    pub fn from_schema(key_schema: Option<FullSchema>, value_schema: FullSchema) -> Result<Module, Error> {
        let mut module = Class::from_existing("Module")
            .send("new", None)
            .try_convert_to::<Module>()
            .unwrap();
        if SchemaKind::from(&value_schema.schema) != SchemaKind::Record {
            return Err(format_err!("Only records "));
        }
        let schema_name = (&value_schema).fullname().unwrap();
        let model_name = RString::new_utf8(&schema_name).send("classify", None);

        ModelDescriptor::new(key_schema, value_schema).map(|descriptor| {
            module.instance_variable_set("@_schema", descriptor);
            module.instance_variable_set("@name", model_name);
            module.define(|itself| {
                itself.def_self("included", rb_included_hook);
            });
            module
        })
    }

    pub fn build_model(schema: FullSchema) -> Class {
        let class = Self::init_model_class(&schema);
        let module = Self::from_schema(None, schema).unwrap();
        class.send("include", Some(&[module.to_any_object()]));
        class
    }

    fn init_model_class(schema: &FullSchema) -> Class {
        let schema_name = schema.fullname().unwrap();
        let model_name = RString::new_utf8(&schema_name)
            .send("classify", None)
            .try_convert_to::<RString>()
            .unwrap()
            .to_string();
        if let Some(class) = ModelRegistry::lookup(&model_name) {
            Self::validate_fingerprints(schema, &model_name, &class);
            return class;
        }

        println!("Allocating model: {:?}", model_name);
        let class = Class::new(&model_name, None);
        ModelRegistry::register(model_name, class.value().into());
        class
    }

    fn validate_fingerprints(schema: &FullSchema, model_name: &str, class: &Class) {
        let schema_name = schema.fullname().unwrap();
        let existing_schema = crate::util::class_ancestor_send(class, "_schema");
        let descriptor = existing_schema.get_data(&*MODEL_DESCRIPTOR_WRAPPER);
        let existing_fingerprint = descriptor.fingerprint();
        println!("{:?}", schema.schema);
        let fingerprint = schema.schema.fingerprint::<Sha256>().bytes;
        if fingerprint != existing_fingerprint {
            println!("{:?}", fingerprint);
            println!("{:?}", existing_fingerprint);
            let message = format!(
                "The {} model is already registered with an incompatible version of the {} schema",
                model_name,
                schema_name,
            );
            VM::raise(Class::from_existing("StandardError"), &message);
        }
    }
}

pub fn initialize() {
    Module::new("AvromaticModelAttributes").define(|itself| {
        itself.def("initialize", rb_initialize);
        itself.def("[]=", rb_set_attribute);
        itself.def("[]", rb_get_attribute);
        itself.def("_attribute_true?", rb_is_attribute_true);
        itself.def("method_missing", rb_method_missing);
        itself.def("respond_to_missing?", rb_respond_to_missing);
        itself.def("avro_message_value", rb_avro_message_value);
        itself.def("attributes", rb_attributes);
        itself.def("to_h", rb_attributes);
        itself.def("to_hash", rb_attributes);
        itself.def("clone", rb_clone);
        itself.def("dup", rb_clone);
        itself.def("hash", rb_hash);
        itself.def("to_s", rb_to_s);
        itself.def("==", rb_equal);
        itself.def("eql?", rb_equal);
    });
    Module::new("AvromaticModel").define(|itself| {
        itself.def_self("build", rb_build);
    });
}

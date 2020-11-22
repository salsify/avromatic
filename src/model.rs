use avro_rs::{FullSchema, schema::{SchemaIter, SchemaKind}};
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use crate::configuration::AvromaticConfiguration;
use crate::heap_guard::HeapGuard;
use crate::descriptors::{ModelDescriptor, MODEL_DESCRIPTOR_WRAPPER};
use crate::model_pool::ModelRegistry;
use crate::schema::{RAvroSchema, ModelSchema, MODEL_SCHEMA_WRAPPER};
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
            standard_error(&err);
            NilClass::new().to_any_object()
        }
    }
}

fn standard_error(err: &Error) {
    let message = format!("{}", err);
    VM::raise(Class::from_existing("StandardError"), &message);
}

fn stringify(object: &AnyObject) -> String {
    object.try_convert_to::<RString>()
        .map(|s| s.to_string())
        .or_else(|_| object.try_convert_to::<Symbol>().map(|s| s.to_string()))
        .unwrap()
}

extern fn rb_initialize(
    argc: Argc,
    argv: *const AnyObject,
    mut itself: AvromaticModelAttributes,
) -> AnyObject {
    // TODO: Ask Kyle about this.
    // itself.call_super(None);
    let arg = RValue::from(0);
    unsafe {
        let p_argv: *const RValue = std::mem::transmute(argv);
        rutie::rubysys::class::rb_scan_args(
            argc,
            p_argv,
            rutie::util::str_to_cstring("01").as_ptr(),
            &arg,
        );
    };
    let arg_obj: AnyObject = arg.into();
    let data: HashMap<String, AnyObject> = if arg_obj.is_nil() {
        HashMap::new()
    } else {
        // TODO: avoid building this hashmap by using the descriptor
        // TODO: need to support non-hash arguments
        println!("Arguments: {}", crate::util::debug_ruby(&arg_obj));
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
    let storage = ModelStorage::default();
    // let mut storage = ModelStorage::default();
    let wrapped_storage: AnyObject = itself.class().wrap_data(storage, &*MODEL_STORAGE_WRAPPER);
    itself.instance_variable_set("@_attributes", wrapped_storage);
    let mut guard = HeapGuard::new();
    descriptor.each_field(|key, attribute| {
        let v = data.get(key).map(Object::to_any_object).unwrap_or(attribute.default());
        if let Err(err) = itself.set_attribute(key, v, &mut guard) {
            let class = Module::from_existing("Avromatic")
                .get_nested_module("Model")
                .get_nested_class("CoercionError");
            let message = format!("error initializing {}: {}", key, err);
            VM::raise(class, &message);
        }
    });
    itself.instance_variable_set("@_constructed", Boolean::new(true));
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
            &args,
        )
    };

    let name_obj = AnyObject::from(name_arg);
    let arguments = Array::from(args);
    let name = argument_check!(name_obj.try_convert_to::<Symbol>());
    let s = name.to_str();

    // predicate
    if s.ends_with("?") {
        let (s, _) = s.split_at(s.len() - 1);
        return itself.is_attribute_true(s);
    }

    // getter
    if !s.ends_with("=") {
        return itself.get_attribute(s);
    }

    // setter is private for an initialized, non-mutable model
    // let constructed = itself.instance_variable_get("@_constructed") == Boolean::new(true).into();
    if false {
        // if !itself.config().is_mutable() && constructed {
        let message = format!(
            "private method `{}' called for {}",
            s,
            itself.class_name().to_str(),
            // class().send("to_s", None).try_convert_to::<RString>().map(|s| s.to_string())
            // .unwrap_or_else(|_| "Object".to_string())
        );
        VM::raise(Class::from_existing("NoMethodError"), &message);
        return NilClass::new().into();
    }

    // Setter
    let (s, _) = s.split_at(s.len() - 1);
    let mut guard = HeapGuard::new();
    if let Err(err) = itself.set_attribute(s, arguments.at(0), &mut guard) {
        let message = format!("{}", err);
        VM::raise(Class::from_existing("ArgumentError"), &message);
    }
    NilClass::new().into()
}


extern fn rb_avro_message_decode(
    argc: Argc,
    argv: *const AnyObject,
    itself: AvromaticModelAttributes,
) -> AnyObject {
    let first_arg = RValue::from(0);
    let second_arg = RValue::from(0);

    unsafe {
        let p_argv: *const RValue = std::mem::transmute(argv);

        rutie::rubysys::class::rb_scan_args(
            argc,
            p_argv,
            rutie::util::str_to_cstring("11").as_ptr(),
            &first_arg,
            &second_arg,
        )
    };

    let first_obj = AnyObject::from(first_arg);
    let second_obj = AnyObject::from(second_arg);
    let result = if second_obj.is_nil() {
        let bytes = argument_check!(first_obj.try_convert_to::<RString>());
        let bytes = bytes.to_bytes_unchecked();
        itself.decode_value(bytes)
    } else {
        let key_bytes = argument_check!(first_obj.try_convert_to::<RString>());
        let key_bytes = key_bytes.to_bytes_unchecked();
        let value_bytes = argument_check!(second_obj.try_convert_to::<RString>());
        let value_bytes = value_bytes.to_bytes_unchecked();
        itself.decode_message(key_bytes, value_bytes)
    };
    raise_if_error(result)
}

methods!(
    AvromaticModelAttributes,
    itself,

    fn rb_attributes() -> AnyObject {
        itself.attribute_hash().into()
    }

    fn rb_key_attributes() -> AnyObject {
        let hash_var = itself.instance_variable_get("@rb_key_attributes");
        if !itself.config().is_mutable() && hash_var.is_nil() {
            let hash = itself.attribute_hash_for_key();
            itself.instance_variable_set("@rb_key_attributes", hash);
            itself.instance_variable_get("@rb_key_attributes")
        } else if !itself.config().is_mutable() {
            hash_var
        } else {
            itself.attribute_hash_for_key().into()
        }
    }

    fn rb_value_attributes() -> AnyObject {
        let hash_var = itself.instance_variable_get("@rb_value_attributes");
        if !itself.config().is_mutable() && hash_var.is_nil() {
            let hash = itself.attribute_hash_for_value();
            itself.instance_variable_set("@rb_value_attributes", hash);
            itself.instance_variable_get("@rb_value_attributes")
        } else if !itself.config().is_mutable() {
            hash_var
        } else {
            itself.attribute_hash_for_value().into()
        }
    }

    fn rb_set_attribute(key: AnyObject, value: AnyObject) -> AnyObject {
        let key = argument_check!(key);
        let key = stringify(&key);
        let value = argument_check!(value);
        let mut guard = HeapGuard::new();
        if let Err(err) = itself.set_attribute(&key, value, &mut guard) {
            let message = format!("{}", err);
            VM::raise(Class::from_existing("ArgumentError"), &message);
        }
        NilClass::new().into()
    }

    fn rb_get_attribute(key: AnyObject) -> AnyObject {
        let key = argument_check!(key);
        let key = stringify(&key);
        itself.get_attribute(&key)
    }

    fn rb_is_attribute_true(key: AnyObject) -> AnyObject {
        let key = argument_check!(key);
        let key = stringify(&key);
        itself.is_attribute_true(&key)
    }

    fn rb_avro_message_value() -> AnyObject {
        let encoding = Encoding::find("ASCII-8BIT").unwrap();
        let result = itself.serialize_value().map(|bytes| RString::from_bytes(&bytes, &encoding));
        raise_if_error(result)
    }

    fn rb_avro_message_key() -> AnyObject {
        let encoding = Encoding::find("ASCII-8BIT").unwrap();
        let result = itself.serialize_key().map(|bytes| RString::from_bytes(&bytes, &encoding));
        raise_if_error(result)
    }

    fn rb_attribute_definitions() -> Hash {
        let schema = itself.protect_public_send("_schema", &[]).unwrap();
        let descriptor = schema.get_data(&*MODEL_DESCRIPTOR_WRAPPER);
        let mut hash = Hash::new();
        descriptor.each_field(|k, _| {
            let key = Symbol::new(k);
            hash.store(key, NilClass::new());
        });
        hash
    }

    fn rb_register_schemas() -> NilClass {
        let config = itself.self_config();
        let key = config.rb_key_schema();
        let value = config.rb_value_schema();
        if let Some(key) = key {
            itself.register_schema(key);
        }
        itself.register_schema(value);
        NilClass::new()
    }

    fn rb_nested_models() -> AnyObject {
        itself.self_config().nested_models().to_any_object()
    }

    fn rb_key_avro_schema() -> AnyObject {
        if let Some(schema) = itself.self_config().rb_key_schema() {
            schema.to_any_object()
        } else {
            NilClass::new().into()
        }
    }

    fn rb_value_avro_schema() -> AnyObject {
        itself.self_config().rb_value_schema().to_any_object()
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
        itself.attribute_hash().protect_public_send("hash", &[]).unwrap()
    }

    fn rb_to_s() -> RString {
        let s = format!(
            "#<{}:{}>",
            itself.class_name().to_str(),
            itself.protect_public_send("object_id", &[]).unwrap().try_convert_to::<Integer>().unwrap().to_i64(),
        );
        RString::new_utf8(&s)
    }

    fn rb_equal(other: AnyObject) -> AnyObject {
        let other = argument_check!(other);
        if itself.class() != other.class() {
            return Boolean::new(false).into();
        }

        let other = unsafe { other.to::<AvromaticModelAttributes>() };
        let other_hash = other.attribute_hash().to_any_object();
        Boolean::new(itself.attribute_hash().equals(&other_hash)).into()
    }

    fn rb_inspect() -> RString {
        let mut parts = Vec::new();
        itself.attribute_hash().each(|k, v| {
            let s = format!(
                "{}: {}",
                k.protect_public_send("to_s", &[]).unwrap().try_convert_to::<RString>().unwrap().to_str(),
                v.protect_public_send("inspect", &[]).unwrap().try_convert_to::<RString>().unwrap().to_str(),
            );
            parts.push(s);
        });
        let inner = parts.join(", ");
        let s = format!("#<{} {}>", itself.class_name().to_str(), inner);
        RString::new_utf8(&s)
    }
);

impl AvromaticModelAttributes {
    fn class_name(&self) -> RString {
        self.class().protect_public_send("name", &[])
            .expect("unexpected exception")
            .try_convert_to::<RString>().unwrap_or_else(|_| RString::new_utf8("Class"))
    }

    fn with_storage<F, R>(&self, f: F) -> R
        where F: FnOnce(&mut ModelStorage) -> R
    {
        let mut attributes = self.instance_variable_get("@_attributes");
        let storage = attributes.get_data_mut(&*MODEL_STORAGE_WRAPPER);
        f(storage)
    }

    fn decode_message(&self, key: &[u8], value: &[u8]) -> Result<AnyObject, Error> {
        let schema = self.protect_public_send("_schema", &[]).expect("unexpected exception");
        let descriptor = schema.get_data(&*MODEL_DESCRIPTOR_WRAPPER);
        let mut guard = HeapGuard::new();
        descriptor.deserialize_message(
            &Class::from(self.value()),
            &key[5..],
            &value[5..],
            &mut guard,
        )
    }

    fn decode_value(&self, bytes: &[u8]) -> Result<AnyObject, Error> {
        let schema = self.protect_public_send("_schema", &[]).expect("unexpected exception");
        let descriptor = schema.get_data(&*MODEL_DESCRIPTOR_WRAPPER);
        let mut guard = HeapGuard::new();
        let mut cursor = std::io::Cursor::new(bytes);
        let _magic = cursor.read_u8()?;
        let schema_id = cursor.read_i32::<BigEndian>()?;
        let mut writer_schema = self.get_schema_by_id(schema_id)?;
        descriptor.deserialize_value(
            &Class::from(self.value()),
            &bytes[5..],
            &writer_schema.rust_schema()?,
            &mut guard,
        )
    }

    fn serialize_key(&self) -> Result<Vec<u8>, Error> {
        let schema = crate::util::ancestor_send(self, "_schema");
        let descriptor = schema.get_data(&*MODEL_DESCRIPTOR_WRAPPER);
        let rb_schema = self.config().rb_key_schema();
        if rb_schema.is_none() {
            return Err(format_err!("Model has no key schema"));
        }
        let id = self.register_schema(rb_schema.unwrap())?;
        let mut buf = Vec::new();
        buf.write_u8(0)?;
        buf.write_i32::<BigEndian>(id as i32)?;
        self.with_storage(|storage| descriptor.serialize_key(&storage.attributes, &mut buf))?;
        Ok(buf)
    }

    fn serialize_value(&self) -> Result<Vec<u8>, Error> {
        let schema = crate::util::ancestor_send(self, "_schema");
        let descriptor = schema.get_data(&*MODEL_DESCRIPTOR_WRAPPER);
        let id = self.register_schema(self.config().rb_value_schema())?;
        let mut buf = Vec::new();
        buf.write_u8(0)?;
        buf.write_i32::<BigEndian>(id as i32)?;
        self.with_storage(|storage| descriptor.serialize_value(&storage.attributes, &mut buf))?;
        Ok(buf)
    }

    pub fn attribute_hash(&self) -> Hash {
        let schema = crate::util::ancestor_send(self, "_schema");
        let descriptor = schema.get_data(&*MODEL_DESCRIPTOR_WRAPPER);
        let mut hash = Hash::new();
        self.with_storage(|storage| {
            descriptor.each_field(|name, descriptor| {
                let v = storage.attributes.get(name).unwrap_or(&AvromaticValue::Null);
                let key = Symbol::new(name);
                let value = descriptor.to_ruby(&v);
                hash.store(key, value);
            });
        });
        hash
    }

    pub fn attribute_hash_for_key(&self) -> Hash {
        let schema = crate::util::ancestor_send(self, "_schema");
        let descriptor = schema.get_data(&*MODEL_DESCRIPTOR_WRAPPER);
        let mut hash = Hash::new();
        self.with_storage(|storage| {
            descriptor.each_key_field(|name, descriptor| {
                let v = storage.attributes.get(name).unwrap_or(&AvromaticValue::Null);
                let key = RString::new_utf8(name);
                let value = descriptor.to_ruby(&v);
                hash.store(key, value);
            });
        });
        hash
    }

    pub fn attribute_hash_for_value(&self) -> Hash {
        let schema = crate::util::ancestor_send(self, "_schema");
        let descriptor = schema.get_data(&*MODEL_DESCRIPTOR_WRAPPER);
        let mut hash = Hash::new();
        self.with_storage(|storage| {
            descriptor.each_value_field(|name, descriptor| {
                let v = storage.attributes.get(name).unwrap_or(&AvromaticValue::Null);
                let key = RString::new_utf8(name);
                let value = descriptor.to_ruby(&v);
                hash.store(key, value);
            });
        });
        hash
    }

    fn set_attribute(
        &self,
        key: &str,
        value: AnyObject,
        guard: &mut HeapGuard,
    ) -> Result<(), Error> {
        let avromatic_value_result = crate::util::ancestor_send(self, "_schema")
            .get_data(&*MODEL_DESCRIPTOR_WRAPPER)
            .coerce(&key, value, guard);
        if let Err(err) = avromatic_value_result {
            return Err(err);
        }

        let value = avromatic_value_result.unwrap();
        self.with_storage(|storage| {
            storage
                .attributes
                .insert(key.to_string(), value.into());
        });
        Ok(())
    }

    fn get_attribute(&self, key: &str) -> AnyObject {
        self.with_storage(|storage| {
            if let Some(value) = storage.attributes.get(key) {
                let schema = crate::util::ancestor_send(self, "_schema");
                let descriptor = schema.get_data(&*MODEL_DESCRIPTOR_WRAPPER);
                descriptor.to_ruby(&key, &value)
            } else {
                NilClass::new().into()
            }
        })
    }


    fn is_attribute_true(&self, key: &str) -> AnyObject {
        self.with_storage(|storage| {
            let stored = storage.attributes.get(key);
            if let Some(AvromaticValue::True) = stored {
                Boolean::new(true)
            } else if let Some(AvromaticValue::Union(1, ref boxed)) = stored {
                Boolean::new(**boxed == AvromaticValue::True)
            } else {
                Boolean::new(false)
            }
        }).to_any_object()
    }

    fn config(&self) -> AvromaticConfiguration {
        unsafe { self.class().instance_variable_get("@config").to() }
    }

    fn self_config(&self) -> AvromaticConfiguration {
        unsafe { self.instance_variable_get("@config").to() }
    }

    fn register_schema(&self, schema: RAvroSchema) -> Result<i64, Error> {
        let fullname = schema.protect_public_send("fullname", &[])
            .expect("unexpected exception");
        let int = Module::from_existing("Avromatic")
            .protect_public_send("messaging", &[]).expect("unexpected exception")
            .protect_public_send("registry", &[]).expect("unexpected exception")
            .protect_public_send("register", &[fullname, schema.to_any_object()]).expect("unexpected exception")
            .try_convert_to::<Integer>()
            .unwrap();
        Ok(int.to_i64())
    }

    fn get_schema_by_id(&self, id: i32) -> Result<RAvroSchema, Error> {
        Module::from_existing("Avromatic")
            .protect_public_send("messaging", &[])
            .expect("unexpected exception")
            .protect_public_send("schema_by_id", &[Integer::new(id as i64).into()])
            .expect("unexpected exception")
            .try_convert_to::<RAvroSchema>()
            .map_err(|_| format_err!("Schema '{}' not found", id))
    }
}

module!(AvromaticModel);

methods!(
    AvromaticModel,
    _itself,

    fn rb_included_hook(model_class: Class) -> AnyObject {
        let mut model_class = argument_check!(model_class);

        let mut config = _itself.instance_variable_get("@config");
        let config = config.try_convert_to::<AvromaticConfiguration>().unwrap();


        let key_schema = rb_try!(config.key_schema());
        let value_schema = rb_try!(config.value_schema());

        model_class.include("AvromaticModelAttributes");
        AvromaticModel::define_class_methods(&mut model_class, &config);

        if config.should_register() {
            let schema_name = (&value_schema).fullname().unwrap();
            let model_name = _itself.instance_variable_get("@name").try_convert_to::<RString>().unwrap().to_string();
            if let Some(ref class) = config.nested_models().lookup(&schema_name) {
                let schema = config.value_schema().unwrap();
                AvromaticModel::validate_fingerprints(&schema, &model_name, class);
            } else {
                config.nested_models().register(&model_class);
            }
        }

        let descriptor = rb_try!(ModelDescriptor::new(key_schema, value_schema, &ModelRegistry::global()));
        model_class.instance_variable_set("@_schema", descriptor.to_any_object());
        model_class.singleton_class().attr_reader("_schema");

        NilClass::new().into()
    }

    fn rb_build(config: AvromaticConfiguration) -> AnyObject {
        let mut config = argument_check!(config);
        config.set_root_model();
        raise_if_error(AvromaticModel::from_config(config).into())
    }
);

impl AvromaticModel {
    fn define_class_methods(class: &mut Class, config: &AvromaticConfiguration) {
        class.instance_variable_set("@config", config.to_any_object());
        class.singleton_class().attr_reader("config");

        class.def_self("avro_message_decode", rb_avro_message_decode);
        // class.def_self("avro_raw_decode", rb_avro_raw_decode);
        class.def_self("attribute_definitions", rb_attribute_definitions);
        class.def_self("key_avro_schema", rb_key_avro_schema);
        class.def_self("avro_schema", rb_value_avro_schema);
        class.def_self("value_avro_schema", rb_value_avro_schema);
        class.def_self("register_schemas!", rb_register_schemas);
        class.def_self("nested_models", rb_nested_models);
    }

    pub fn from_config(config: AvromaticConfiguration) -> Result<Module, Error> {
        let value_schema = config.value_schema()?;

        let mut module = Self::from_schema(value_schema)?;
        module.instance_variable_set("@config", config);
        Ok(module)
    }

    fn from_schema(value_schema: FullSchema) -> Result<Module, Error> {
        let mut module = Class::from_existing("Module")
            .protect_public_send("new", &[])
            .expect("unexpected exception")
            .try_convert_to::<Module>()
            .unwrap();
        let kind = SchemaKind::from(&value_schema.schema);
        if kind != SchemaKind::Record {
            let err = format_err!(
                "Unsupported schema type '{}', only '{}' schemas are supported.",
                kind.type_name(),
                SchemaKind::Record.type_name(),
            );
            return Err(err);
        }
        let schema_name = (&value_schema).fullname().unwrap();
        let model_name = RString::new_utf8(&schema_name)
            .protect_public_send("classify", &[])
            .expect("unexpected exception");

        module.instance_variable_set("@name", model_name);
        module.define(|itself| {
            itself.def_self("included", rb_included_hook);
        });
        Ok(module)
    }

    pub fn build_model(
        schema: FullSchema,
        nested_models: &ModelRegistry,
    ) -> Class {
        let schema_name = (&schema).fullname().unwrap();
        let model_name = RString::new_utf8(&schema_name)
            .protect_public_send("classify", &[])
            .expect("unexpected exception")
            .try_convert_to::<RString>()
            .unwrap()
            .to_string();
        if let Some(class) = ModelRegistry::global().lookup(&schema_name) {
            Self::validate_fingerprints(&schema, &model_name, &class);
            return class;
        }

        let mut class = Class::from_existing("Class").new_instance(&[]).try_convert_to::<Class>().unwrap();
        let rb_schema = ModelSchema { schema: schema.clone() };
        let avro_schema: AnyObject = Class::from_existing("AvroSchema")
            .wrap_data(rb_schema, &*MODEL_SCHEMA_WRAPPER);
        class.instance_variable_set("@__schema", avro_schema);
        let config = AvromaticConfiguration::new(&schema, nested_models).unwrap();
        Self::define_class_methods(&mut class, &config);
        config.nested_models().register(&class);
        let mut module = Self::from_schema(schema).unwrap();
        module.instance_variable_set("@config", config);
        // TODO fails test suite with:
        // thread '<unnamed>' panicked at 'called `Result::unwrap()` on an `Err` value: #<StandardError: Failed to convert avro 'Long(0)' to Int>', src/model.rs:691:73
        //
        // Unclear how to bubble Ruby-facing error up through this interface, or why this is in the stack trace.
        class.protect_public_send("include", &[module.to_any_object()]).unwrap();
        class
    }

    fn validate_fingerprints(schema: &FullSchema, model_name: &str, class: &Class) {
        let schema_name = schema.fullname().unwrap();
        let schema_var = class.instance_variable_get("@__schema");
        if schema_var.is_nil() {
            // not a nested model
            return;
        }
        let existing_schema = schema_var.get_data(&*MODEL_SCHEMA_WRAPPER);
        let existing_fingerprint = existing_schema.schema.schema.fingerprint::<Sha256>().bytes;
        let fingerprint = schema.schema.fingerprint::<Sha256>().bytes;
        if fingerprint != existing_fingerprint {
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
        itself.def("avro_message_key", rb_avro_message_key);
        itself.def("avro_message_value", rb_avro_message_value);
        itself.def("avro_raw_key", rb_avro_message_key);
        itself.def("avro_raw_value", rb_avro_message_value);
        itself.def("attributes", rb_attributes);
        itself.def("avro_key_datum", rb_key_attributes);
        itself.def("avro_value_datum", rb_value_attributes);
        itself.def("to_h", rb_attributes);
        itself.def("to_hash", rb_attributes);
        itself.def("clone", rb_clone);
        itself.def("dup", rb_clone);
        itself.def("hash", rb_hash);
        itself.def("to_s", rb_to_s);
        itself.def("inspect", rb_inspect);
        itself.def("==", rb_equal);
        itself.def("eql?", rb_equal);
    });
    Module::new("AvromaticModel").define(|itself| {
        itself.def_self("build", rb_build);
    });
    Class::new("AvroSchema", None);
}


macro_rules! argument_check {
    ($inner:expr) => {
        match $inner {
            Ok(value) => value,
            Err(e) => {
                let message = format!("Bad argument: {}", e);
                rutie::VM::raise(rutie::Class::from_existing("ArgumentError"), &message);
                return rutie::NilClass::new().into();
            }
        }
    }
}

macro_rules! ruby_class {
    ($ruby_name:expr, $rust_name:ident) => {
        #[derive(Clone, Debug)]
        pub struct $rust_name {
            value: Value,
        }

        impl From<Value> for $rust_name {
            fn from(value: Value) -> Self {
                $rust_name { value }
            }
        }

        impl Into<Value> for $rust_name {
            fn into(self) -> Value {
                self.value
            }
        }

        impl Borrow<Value> for $rust_name {
            fn borrow(&self) -> &Value {
                &self.value
            }
        }

        impl AsRef<Value> for $rust_name {
            fn as_ref(&self) -> &Value {
                &self.value
            }
        }

        impl AsRef<$rust_name> for $rust_name {
            #[inline]
            fn as_ref(&self) -> &Self {
                self
            }
        }

        impl Object for $rust_name {
            #[inline]
            fn value(&self) -> Value {
                self.value
            }
        }

        impl Deref for $rust_name {
            type Target = Value;

            fn deref(&self) -> &Value {
                &self.value
            }
        }

        impl VerifiedObject for $rust_name {
            fn is_correct_type<T: Object>(obj: &T) -> bool {
                instance_of(obj, Class::from_existing($ruby_name))
            }

            fn error_message() -> &'static str {
                "Error converting to Time"
            }
        }

        impl PartialEq for $rust_name {
            fn eq(&self, other: &Self) -> bool {
                self.equals(other)
            }
        }
    }
}

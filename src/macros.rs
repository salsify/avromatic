
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

macro_rules! rb_try {
    ($inner:expr) => {
        match $inner {
            Ok(value) => value,
            Err(e) => {
                let message = format!("{}", e);
                rutie::VM::raise(rutie::Class::from_existing("StandardError"), &message);
                return rutie::NilClass::new().into();
            }
        }
    }
}

macro_rules! ruby_class {
    ($rust_name:ident, $ruby_name:expr) => {
        ruby_class!($rust_name, $ruby_name, Class::from_existing($ruby_name));
    };
    ($rust_name:ident, $ruby_name:expr, $ruby_class:expr) => {
        ruby_class!(@ $rust_name, $ruby_name, $ruby_class, validator, class_method);
    };
    (@ $rust_name:ident, $ruby_name:expr, $ruby_class:expr $(, $features:ident)*) => {
        #[derive(Clone, Debug)]
        pub struct $rust_name {
            value: rutie::types::Value,
        }

        impl From<rutie::types::Value> for $rust_name {
            fn from(value: rutie::types::Value) -> Self {
                $rust_name { value }
            }
        }

        impl Into<rutie::types::Value> for $rust_name {
            fn into(self) -> rutie::types::Value {
                self.value
            }
        }

        impl std::borrow::Borrow<rutie::types::Value> for $rust_name {
            fn borrow(&self) -> &rutie::types::Value {
                &self.value
            }
        }

        impl AsRef<rutie::types::Value> for $rust_name {
            fn as_ref(&self) -> &rutie::types::Value {
                &self.value
            }
        }

        impl AsRef<$rust_name> for $rust_name {
            #[inline]
            fn as_ref(&self) -> &Self {
                self
            }
        }

        impl rutie::Object for $rust_name {
            #[inline]
            fn value(&self) -> rutie::types::Value {
                self.value
            }
        }

        impl std::ops::Deref for $rust_name {
            type Target = rutie::types::Value;

            fn deref(&self) -> &rutie::types::Value {
                &self.value
            }
        }

        $(
            ruby_class!(@ $features, $rust_name, $ruby_name, $ruby_class);
        )*

        impl PartialEq for $rust_name {
            fn eq(&self, other: &Self) -> bool {
                use rutie::Object;
                self.equals(other)
            }
        }
    };
    (@ class_method, $rust_name:ident, $ruby_name:expr, $ruby_class:expr) => {
        impl $rust_name {
            #[allow(dead_code)]
            pub fn class() -> Class {
                $ruby_class
            }
        }
    };
    (@ validator, $rust_name:ident, $ruby_name:expr, $ruby_class:expr) => {
        impl rutie::VerifiedObject for $rust_name {
            fn is_correct_type<T: rutie::Object>(obj: &T) -> bool {
                $crate::util::instance_of(obj, $ruby_class)
            }

            fn error_message() -> &'static str {
                concat!("Error converting to ", $ruby_name)
            }
        }
    };
}

macro_rules! argument_check {
    ($inner:expr) => {
        match $inner {
            Ok(value) => value,
            Err(e) => {
                raise!("ArgumentError", "Bad argument: {}", e);

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
                raise!("StandardError", e);

                return rutie::NilClass::new().into();
            }
        }
    }
}

/// Raise an AnyException from a Result<AnyObject, AnyException> if one is present and return
/// nil as AnyObject from current function.
macro_rules! rb_try_ex {
    ($inner:expr) => {
        match $inner {
            Ok(value) => value,
            Err(e) => {
                rutie::VM::raise_ex(e);
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

macro_rules! varargs {
    ($fmt:literal, $argc:expr, $argv:expr, $( $ident: ident ),*) => {
        $(
            let $ident = RValue::from(0);
        )*

        unsafe {
            let p_argv: *const RValue = std::mem::transmute($argv);

            rutie::rubysys::class::rb_scan_args(
                $argc,
                p_argv,
                rutie::util::str_to_cstring($fmt).as_ptr(),
                $(
                    &$ident,
                )*
            );
        };
    };
}

macro_rules! raise {
    ($class: literal, $displayable: expr) => {
        raise!($class, "{}", $displayable);
    };

    ($class: literal, $fmt: literal, $( $placeholder: expr ),*) => {
        let class = Class::from(rutie::util::inmost_rb_object($class));

        let message = format!($fmt, $( $placeholder, )*);

        rutie::VM::raise(class, &message);
    };
}
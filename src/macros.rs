
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

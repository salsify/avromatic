extern crate rutie;
#[macro_use]
extern crate lazy_static;

#[macro_use]
mod macros;

mod configuration;
mod custom_types;
mod descriptors;
mod heap_guard;
mod model;
mod model_pool;
mod serializer;
mod util;
mod values;

#[allow(non_snake_case)]
#[no_mangle]
pub extern "C" fn Init_avromatic() {
    descriptors::initialize();
    model::initialize();
    model_pool::initialize();
}

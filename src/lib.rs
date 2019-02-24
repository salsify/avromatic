extern crate rutie;
#[macro_use]
extern crate lazy_static;

#[macro_use]
mod macros;

mod configuration;
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

//
// # creates registry
// define_avro_model do
//   attribute :system_id, :salsify_uuid
//   attribute :system_message_timestamp, :timestamp
//   attribute :created_at, :timestamp
//   attribute :updated_at, :timestamp
//   attribute :destroyed_at, [:nil, :timestamp]
//   attribute :list_memberships, :array, values: :string
//   attribute :property_value_collections, :array, values: [:string_pvals, :reference_pvals]
// end


// class Product::Model
//   avro_attribute :system_id, :salsify_uuid

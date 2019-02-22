# frozen_string_literal: true

require 'avromatic'
require 'salsify_avro'
require 'benchmark/ips'
require 'benchmark/memory'

# class Reference
#   include Avromatic::Model.build(
#     schema_name: 'com.salsify.reference',
#     native: true
#   )
# end

SalsifyAvro.add_path('/home/kphelps/salsify/salsify_avro/avro/schema')
SalsifyAvro.add_path('/home/kphelps/salsify/dandelion/schemas_gem/avro/schema')
Avromatic.configure do |config|
  config.schema_store = SalsifyAvro.build_schema_store
  config.nested_models = Avromatic::ModelRegistry.new(
    remove_namespace_prefix: 'com.salsify.'
  )
  # config.eager_load_models = [
  #   Reference
  # ]
  config.registry_url = 'http://dandelion:avro@localhost:21000'
  config.build_messaging!
end

class X
  include Avromatic::Model.build(
    schema_name: 'com.salsify.core.product_updated_event_value'
  )
end

class Y
  include Avromatic::Model.build(
    schema_name: 'com.salsify.core.product_updated_event_value',
    native: false
  )
end

values = {
  system_message_id: SalsifyUuid.generate.to_s,
  system_message_timestamp: Time.now,
  organization_id: SalsifyUuid.generate.to_s,
  product: {
    id: SalsifyUuid.generate.to_s,
    type: 'products'
  },
  property_value_modifications: [
    {
      property: {
        id: SalsifyUuid.generate.to_s,
        type: 'properties'
      },
      new_value_data_type: 'string',
      new_values: {
        values: []
      }
    }
  ] * 1000
}

x_values = {
  **values,
  system_message_timestamp: values[:system_message_timestamp].to_i
}
x = X.new(x_values.deep_dup)
y = Y.new(values.deep_dup)

y_data = y.avro_message_value
x_data = x.avro_message_value

Y.avro_message_decode(y_data).avro_message_value

puts '--- Ctor -> Ser (publishing use case) ---'
Benchmark.ips do |z|
  z.report('ruby') { Y.new(values).avro_message_value }
  z.report('rust') { X.new(x_values).avro_message_value }
  z.compare!
end

puts '--- De ---'
Benchmark.ips do |z|
  z.report('ruby') { Y.avro_message_decode(y_data) }
  z.report('rust') { X.avro_message_decode(x_data) }
  z.compare!
end

puts '--- Ctor ---'
Benchmark.ips do |z|
  z.report('ruby') { Y.new(values) }
  z.report('rust') { X.new(x_values) }
  z.compare!
end

puts '--- Ser ---'
Benchmark.ips do |z|
  z.report('ruby') { y.avro_message_value }
  z.report('rust') { x.avro_message_value }
  z.compare!
end

puts '--- De -> Ser (mutable state use case) ---'
Benchmark.ips do |z|
  z.report('ruby') { Y.avro_message_decode(y_data).avro_message_value }
  z.report('rust') { X.avro_message_decode(x_data).avro_message_value }
  z.compare!
end

# frozen_string_literal: true

require 'avromatic'
require 'salsify_avro'
require 'benchmark/ips'
require 'benchmark/memory'
require 'pry'

SalsifyAvro.add_path('/Users/kylesmith/salsify_avro/avro/schema')
SalsifyAvro.add_path('/Users/kylesmith/dandelion/schemas_gem/avro/schema')
Avromatic.configure do |config|
  config.register_type('com.salsify.salsify_uuid', SalsifyUuid) do |type|
    type.from_avro = ->(value) { SalsifyUuid.parse(value) }
    type.to_avro = ->(value) { value.to_s }
  end

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
    key_schema_name: 'com.salsify.core.product_event_key',
    value_schema_name: 'com.salsify.core.product_updated_event_value'
  )
end

class Y
  include Avromatic::Model.build(
    key_schema_name: 'com.salsify.core.product_event_key',
    value_schema_name: 'com.salsify.core.product_updated_event_value',
    native: false
  )
end

product_id = SalsifyUuid.generate
values = {
  system_message_id: SalsifyUuid.generate.to_s,
  system_message_timestamp: Time.now,
  organization_id: SalsifyUuid.generate,
  product: {
    id: product_id,
    type: 'products'
  },
  product_id: product_id,
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
  ] * 100
}

x = X.new(values.deep_dup)
y = Y.new(values.deep_dup)

y_key = y.avro_message_key
y_value = y.avro_message_value
x_key = x.avro_message_key
x_value = x.avro_message_value

puts y_key == x_key
puts y_value == x_value
binding.pry

Y.avro_message_decode(y_key, y_value).avro_message_key
Y.avro_message_decode(y_key, y_value).avro_message_value

puts '--- De -> Ser (mutable state use case) ---'
Benchmark.ips do |z|
  z.report('ruby') { Y.avro_message_decode(y_key, y_value).avro_message_value }
  z.report('rust') { X.avro_message_decode(x_key, x_value).avro_message_value }
  z.compare!
end

puts '--- Ctor -> Ser (publishing use case) ---'
Benchmark.ips do |z|
  z.report('ruby') do
    a = Y.new(values)
    a.avro_message_key
    a.avro_message_value
  end
  z.report('rust') do
    a = X.new(values)
    a.avro_message_key
    a.avro_message_value
  end
  z.compare!
end

puts '--- De ---'
Benchmark.ips do |z|
  z.report('ruby') { Y.avro_message_decode(y_key, y_value) }
  z.report('rust') { X.avro_message_decode(x_key, x_value) }
  z.compare!
end

puts '--- Ctor ---'
Benchmark.ips do |z|
  z.report('ruby') { Y.new(values) }
  z.report('rust') { X.new(values) }
  z.compare!
end

puts '--- Value Ser ---'
Benchmark.ips do |z|
  z.report('ruby') { y.avro_message_value }
  z.report('rust') { x.avro_message_value }
  z.compare!
end

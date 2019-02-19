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

SalsifyAvro.add_path('/Users/kphelps/sandbox/salsify_avro/avro/schema')
SalsifyAvro.add_path('/Users/kphelps/sandbox/dandelion/schemas_gem/avro/schema')
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
    schema_name: 'com.salsify.core.product_updated_event_value',
    native: true
  )
end

class Y
  include Avromatic::Model.build(
    schema_name: 'com.salsify.core.product_updated_event_value'
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
  property_value_modifications: []
  # property_value_modifications: [
  #   {
  #     property: {
  #       id: SalsifyUuid.generate.to_s,
  #       type: 'properties'
  #     },
  #     new_value_data_type: 'string',
  #     new_values: {
  #       values: []
  #     }
  #   }
  # ] * 1
}

x_values = {
  **values,
  system_message_timestamp: values[:system_message_timestamp].to_i
}
puts "X ctor"
x = X.new(x_values.deep_dup)
puts "Y ctor"
y = Y.new(values.deep_dup)

# puts "X ser"
# puts x['system_message_timestamp']

y_data = y.avro_message_value
x_data = x.avro_message_value
# puts "Y ser"
# puts y.avro_message_value.inspect[5...-1]
# puts x.avro_message_value == y.avro_message_value[5...-1]
# puts Y.avro_message_decode(y.avro_message_value[0...5] + x.avro_message_value).inspect
# puts y.inspect

Benchmark.ips do |z|
  z.report('ruby') { Y.avro_message_decode(y_data).avro_message_value }
  z.report('rust') { X.avro_message_decode(x_data).avro_message_value }
  z.compare!
end

Benchmark.ips do |z|
  z.report('ruby') { Y.avro_message_decode(y_data) }
  z.report('rust') { X.avro_message_decode(x_data) }
  z.compare!
end

Benchmark.ips do |z|
  z.report('ruby') { Y.new(values) }
  z.report('rust') { X.new(x_values) }
  z.compare!
end

Benchmark.ips do |z|
  z.report('ruby') { y.avro_message_value }
  z.report('rust') { x.avro_message_value }
  z.compare!
end

# Benchmark.memory do |z|
#   z.report('ruby') { Y.new(values).avro_message_value }
#   z.report('rust') { X.new(x_values).avro_message_value }
#   z.compare!
# end

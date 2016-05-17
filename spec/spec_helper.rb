$LOAD_PATH.unshift File.expand_path('../../lib', __FILE__)
require 'simplecov'

SimpleCov.start

require 'avromatic'

RSpec.configure do |config|
  config.before do
    Avromatic.logger = Logger.new('log/test.log')
    Avromatic.registry_url = 'http://registry.example.com'
    Avromatic.schema_store = AvroTurf::SchemaStore.new(path: 'spec/avro/schema')
    Avromatic.build_messaging!
    Avromatic.type_registry.clear
  end
end

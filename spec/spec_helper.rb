$LOAD_PATH.unshift File.expand_path('../../lib', __FILE__)
require 'simplecov'

SimpleCov.start do
  add_filter 'spec'
  minimum_coverage 98
end

require 'avromatic'

Dir["#{File.dirname(__FILE__)}/support/**/*.rb"].each { |f| require f }

RSpec.configure do |config|
  config.extend LogicalTypesHelper

  config.before do
    Avromatic.logger = Logger.new('log/test.log')
    Avromatic.registry_url = 'http://registry.example.com'
    Avromatic.schema_store = AvroTurf::SchemaStore.new(path: 'spec/avro/schema')
    Avromatic.build_messaging!
    Avromatic.type_registry.clear
  end
end

# This needs to be required after the before block that sets
# Avromatic.registry_url
require 'avromatic/rspec'

# frozen_string_literal: true

$LOAD_PATH.unshift File.expand_path('../lib', __dir__)
require 'simplecov'

SimpleCov.start do
  add_filter 'spec'
  minimum_coverage 95
end

require 'avro/builder'
require 'avromatic'
require 'active_support/core_ext/hash/keys'

Dir["#{File.dirname(__FILE__)}/support/**/*.rb"].sort.each { |f| require f }

RSpec.configure do |config|
  config.extend LogicalTypesHelper

  config.before do
    Avromatic.logger = Logger.new('log/test.log')
    Avromatic.registry_url = 'http://username:password@registry.example.com'
    Avromatic.use_schema_fingerprint_lookup = true
    Avromatic.schema_store = AvroTurf::SchemaStore.new(path: 'spec/avro/schema')
    Avromatic.custom_type_registry.clear
    Avromatic.nested_models = Avromatic::ModelRegistry.new

    Time.zone = 'GMT'
  end

  config.filter_run_when_matching :focus
end

# This needs to be required after the before block that sets
# Avromatic.registry_url
require 'avromatic/rspec'

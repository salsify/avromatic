# frozen_string_literal: true

require 'webmock/rspec'
require 'avro_schema_registry/test/fake_server'

RSpec.configure do |config|
  config.before(:each) do
    WebMock.stub_request(:any, /^#{Avromatic.registry_url}/).to_rack(AvroSchemaRegistry::FakeServer)
    AvroSchemaRegistry::FakeServer.clear
    Avromatic.build_messaging!
  end
end

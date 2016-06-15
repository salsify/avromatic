require 'webmock/rspec'
require 'avro_turf/test/fake_schema_registry_server'

RSpec.configure do |config|
  config.before(:each) do
    WebMock.stub_request(:any, /^#{Avromatic.registry_url}/).to_rack(FakeSchemaRegistryServer)
    FakeSchemaRegistryServer.clear
  end
end

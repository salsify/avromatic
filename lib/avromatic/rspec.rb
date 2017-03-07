require 'webmock/rspec'
require 'avromatic/test/fake_schema_registry_server'

RSpec.configure do |config|
  config.before(:each) do
    WebMock.stub_request(:any, /^#{Avromatic.registry_url}/).to_rack(FakeConfluentSchemaRegistryServer)
    FakeConfluentSchemaRegistryServer.clear
  end
end

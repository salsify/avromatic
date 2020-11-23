# frozen_string_literal: true

require 'uri'
require 'webmock/rspec'
require 'avro_schema_registry/test/fake_server'

RSpec.configure do |config|
  config.before(:each) do
    # Strip the username/password from the URL so WebMock can match the URL
    registry_uri = URI(Avromatic.registry_url)
    registry_uri.userinfo = ''

    WebMock.stub_request(:any, /^#{registry_uri}/).to_rack(AvroSchemaRegistry::FakeServer)
    AvroSchemaRegistry::FakeServer.clear
    Avromatic.build_messaging!
  end
end

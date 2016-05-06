require 'spec_helper'
require 'webmock/rspec'
require 'avro_turf/test/fake_schema_registry_server'

describe Avromatic::Model::Serialization do
  let(:registry_url) { 'http://registry.example.com' }
  let(:values) { { id: rand(99) } }
  let(:instance) { test_class.new(values) }

  before do
    Avromatic.registry_url = registry_url
    stub_request(:any, /^#{registry_url}/).to_rack(FakeSchemaRegistryServer)
    FakeSchemaRegistryServer.clear
  end

  describe "#avro_message_value" do
    let(:test_class) do
      Avromatic::Model.model(value_schema_name: 'test.encode_value')
    end
    let(:values) { { str1: 'a', str2: 'b' } }

    it "encodes the value for the model" do
      message_value = instance.avro_message_value
      decoded = test_class.deserialize(message_value)
      expect(decoded).to eq(instance)
    end
  end

  describe "#avro_message_key" do
    let(:test_class) do
      Avromatic::Model.model(
        value_schema_name: 'test.encode_value',
        key_schema_name: 'test.encode_key')
    end
    let(:values) { super().merge!(str1: 'a', str2: 'b') }

    it "encodes the key for the model" do
      message_value = instance.avro_message_value
      message_key = instance.avro_message_key
      decoded = test_class.deserialize(message_key, message_value)
      expect(decoded).to eq(instance)
    end

    context "when a model does not have a key schema" do
      let(:test_class) do
        Avromatic::Model.model(value_schema_name: 'test.encode_value')
      end
      let(:values) { { str1: 'a', str2: 'b' } }

      it "raises an error" do
        expect { instance.avro_message_key }.to raise_error('Model has no key schema')
      end
    end
  end

  describe ".deserialize" do
    let(:test_class) do
      Avromatic::Model.model(value_schema_name: 'test.encode_value')
    end
    let(:values) { { str1: 'a', str2: 'b' } }
    let(:avro_message_value) { instance.avro_message_value }

    it "deserializes a model" do
      decoded = test_class.deserialize(avro_message_value)
      expect(decoded).to eq(instance)
    end

    context "when a value and a key are specified" do
      let(:test_class) do
        Avromatic::Model.model(
          value_schema_name: 'test.encode_value',
          key_schema_name: 'test.encode_key')
      end
      let(:values) { { id: rand(99), str1: 'a', str2: 'b' } }
      let(:avro_message_key) { instance.avro_message_key }

      it "deserializes a model" do
        decoded = test_class.deserialize(avro_message_key, avro_message_value)
        expect(decoded).to eq(instance)
      end
    end
  end
end

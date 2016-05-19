require 'spec_helper'
require 'webmock/rspec'
require 'avro_turf/test/fake_schema_registry_server'
require 'avro/builder'

describe Avromatic::Model::Serialization do
  let(:registry_url) { 'http://registry.example.com' }
  let(:values) { { id: rand(99) } }
  let(:instance) { test_class.new(values) }
  let(:avro_message_value) { instance.avro_message_value }
  let(:avro_message_key) { instance.avro_message_key }

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

    context "with a nested record" do
      let(:test_class) do
        Avromatic::Model.model(value_schema_name: 'test.nested_record')
      end
      let(:values) { { str: 'a', sub: { str: 'b', i: 1 } } }

      it "encodes the value for the model" do
        message_value = instance.avro_message_value
        decoded = test_class.deserialize(message_value)
        expect(decoded).to eq(instance)
      end
    end
  end

  describe "#avro_message_key" do
    let(:test_class) do
      Avromatic::Model.model(
        value_schema_name: 'test.encode_value',
        key_schema_name: 'test.encode_key'
      )
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

    it "deserializes a model" do
      decoded = test_class.deserialize(avro_message_value)
      expect(decoded).to eq(instance)
    end

    context "when a value and a key are specified" do
      let(:test_class) do
        Avromatic::Model.model(
          value_schema_name: 'test.encode_value',
          key_schema_name: 'test.encode_key'
        )
      end
      let(:values) { { id: rand(99), str1: 'a', str2: 'b' } }

      it "deserializes a model" do
        decoded = test_class.deserialize(avro_message_key, avro_message_value)
        expect(decoded).to eq(instance)
      end
    end
  end

  context "custom types" do
    let(:schema_name) { 'test.named_type' }
    let(:test_class) do
      Avromatic::Model.model(schema_name: schema_name)
    end
    let(:values) { { six_str: 'fOObAR' } }

    context "with a value class" do
      let(:value_class) do
        Class.new do
          attr_reader :value

          def initialize(value)
            @value = value
          end

          def self.from_avro(value)
            new(value.downcase)
          end

          def self.to_avro(value)
            value.value.capitalize
          end
        end
      end

      before do
        Avromatic.register_type('test.six', value_class)
      end

      it "stores the attribute in the model class" do
        expect(instance.six_str).to be_a(value_class)
      end

      it "converts when assigning to the model" do
        expect(instance.six_str.value).to eq('foobar')
      end

      it "converts when encoding the value" do
        decoded = Avromatic.messaging.decode(avro_message_value, schema_name: schema_name)
        expect(decoded['six_str']).to eq('Foobar')
      end
    end

    context "without a value class" do
      before do
        Avromatic.register_type('test.six') do |type|
          type.from_avro = ->(value) { value.downcase }
          type.to_avro = ->(value) { value.capitalize }
        end
      end

      it "converts when assigning to the model" do
        expect(instance.six_str).to eq('foobar')
      end

      it "converts when encoding the value" do
        decoded = Avromatic.messaging.decode(avro_message_value, schema_name: schema_name)
        expect(decoded['six_str']).to eq('Foobar')
      end
    end

    context "custom type in a union" do
      let(:values) { { optional_six: 'fOObAR' } }

      before do
        Avromatic.register_type('test.six') do |type|
          type.from_avro = ->(value) { value.downcase }
          type.to_avro = ->(value) { value.capitalize }
        end
      end

      it "converts when assigning to the model" do
        expect(instance.optional_six).to eq('foobar')
      end

      it "converts when encoding the value" do
        decoded = Avromatic.messaging.decode(avro_message_value, schema_name: schema_name)
        expect(decoded['optional_six']).to eq('Foobar')
      end
    end

    context "custom type for record" do
      let(:schema_name) { 'test.with_varchar' }
      let(:test_class) do
        Avromatic::Model.model(schema_name: schema_name)
      end
      let(:values) { { str: 'test' } }

      before do
        Avromatic.register_type('test.varchar', String) do |type|
          type.from_avro = ->(value) do
            value.is_a?(String) ? value : value['data']
          end
          type.to_avro = ->(value) do
            { 'data' => value, 'length' => value.size }
          end
        end
      end

      it "stores the attribute" do
        expect(instance.str).to eq('test')
      end

      it "converts when encoding the value" do
        decoded = Avromatic.messaging.decode(avro_message_value, schema_name: schema_name)
        expect(decoded['str']).to eq('length' => 4, 'data' => 'test')
      end

      it "converts when assigning to the model" do
        decoded = test_class.deserialize(avro_message_value)
        expect(decoded.str).to eq('test')
      end
    end
  end
end

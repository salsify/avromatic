require 'spec_helper'
require 'webmock/rspec'
require 'avro_turf/test/fake_schema_registry_server'

describe Avromatic::Model::Decoder do
  let(:registry_url) { 'http://registry.example.com' }

  let(:instance) { described_class.new(*models) }
  let(:model1) do
    Avromatic::Model.model(
      value_schema_name: 'test.encode_value',
      key_schema_name: 'test.encode_key')
  end
  let(:model2) do
    Avromatic::Model.model(value_schema_name: 'test.value')
  end

  before do
    Avromatic.registry_url = registry_url
    stub_request(:any, /^#{registry_url}/).to_rack(FakeSchemaRegistryServer)
    FakeSchemaRegistryServer.clear
  end

  describe "#initialize" do
    context "when multiple models use the same schema" do
      let(:model3) do
        Avromatic::Model.model(
          value_schema_name: 'test.encode_value',
          key_schema_name: 'test.encode_key')
      end
      let(:models) { [model1, model3] }

      it "raises an error" do
        expect { instance }.
          to raise_error(described_class::DuplicateKeyError,
                         /Multiple models \[.*\] have the same key .*/)
      end
    end

    context "when the same model is used multiple times" do
      let(:models) { Array.new(2) { model1 } }

      it "does not raise an error" do
        expect { instance }.not_to raise_error
      end
    end
  end

  describe "#decode" do
    let(:models) { [model1, model2] }
    let(:model1_instance) { model1.new(str1: 'A', str2: 'B', id: 99) }
    let(:model1_value) do
      model1_instance.avro_message_value
    end
    let(:model1_key) do
      model1_instance.avro_message_key
    end
    let(:model2_value) { model2.new(id: 100, action: :CREATE).avro_message_value }

    it "decodes message value and key pairs to registered models" do
      expect(instance.decode(model1_key, model1_value)).to be_a(model1)
      expect(instance.decode(model2_value)).to be_a(model2)
    end

    context "when the message_value does not begin with the magic byte" do
      it "raises an error" do
        expect do
          instance.decode('X')
        end.to raise_error(described_class::MagicByteError,
                           "Expected data to begin with a magic byte, got 'X'")
      end
    end

    context "when the schema name is not known by the decoder" do
      let(:unknown_model) do
        Avromatic::Model.model(schema_name: 'test.defaults')
      end
      let(:message_value) do
        unknown_model.new.avro_message_value
      end

      it "raises an error" do
        expect do
          instance.decode(message_value)
        end.to raise_error(described_class::UnexpectedKeyError,
                           "Unexpected schemas [nil, \"test.defaults\"]")
      end
    end

    context "when the decoder is initialized with a schema registry" do
      let(:schema_registry) { Avromatic.build_schema_registry }
      let(:instance) { described_class.new(*models, schema_registry: schema_registry) }

      before do
        allow(schema_registry).to receive(:fetch).and_call_original
        instance.decode(model1_key, model1_value)
      end

      it "calls find on the provided schema registry" do
        expect(schema_registry).to have_received(:fetch).at_least(1).times
      end
    end
  end
end

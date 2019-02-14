# frozen_string_literal: true

require 'spec_helper'

describe Avromatic::Model::MessageDecoder do
  let(:instance) { described_class.new(*models) }
  let(:model1) do
    Avromatic::Model.model(
      value_schema_name: 'test.encode_value',
      key_schema_name: 'test.encode_key'
    )
  end
  let(:model2) do
    Avromatic::Model.model(value_schema_name: 'test.value')
  end

  describe "#initialize" do
    context "when multiple models use the same schema" do
      let(:model3) do
        Avromatic::Model.model(
          value_schema_name: 'test.encode_value',
          key_schema_name: 'test.encode_key'
        )
      end
      let(:models) { [model1, model3] }

      it "raises an error" do
        expect { instance }
          .to raise_error(described_class::DuplicateKeyError,
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

  shared_examples_for "decoding failure cases" do |method_name|
    context "when the message_value does not begin with the magic byte" do
      it "raises an error" do
        expect do
          instance.send(method_name, 'X')
        end.to raise_error(described_class::MagicByteError,
                           "Expected data to begin with a magic byte, got 'X'")
      end
    end

    context "when the schema name is not known by the decoder" do
      let(:unknown_model) do
        Avromatic::Model.model(
          value_schema_name: 'test.defaults',
          key_schema_name: 'test.encode_key'
        )
      end
      let(:key_value) do
        unknown_model.new(id: 0).avro_message_key
      end
      let(:message_value) do
        unknown_model.new(id: 0).avro_message_value
      end

      it "raises an UnexpectedKeyError when the unknown model only has a value schema" do
        expect do
          instance.send(method_name, message_value)
        end.to raise_error do |error|
          expect(error).to be_a(described_class::UnexpectedKeyError)
          expect(error.message).to eq('Unexpected schemas [nil, "test.defaults"]')
          expect(error.key_schema_name).to be_nil
          expect(error.value_schema_name).to eq('test.defaults')
        end
      end

      it "raises an UnexpectedKeyError when the unknown model has a key schema and a value schema" do
        expect do
          instance.send(method_name, key_value, message_value)
        end.to raise_error do |error|
          expect(error).to be_a(described_class::UnexpectedKeyError)
          expect(error.message).to eq('Unexpected schemas ["test.encode_key", "test.defaults"]')
          expect(error.key_schema_name).to eq('test.encode_key')
          expect(error.value_schema_name).to eq('test.defaults')
        end
      end
    end
  end

  describe "#model" do
    let(:models) { [model1, model2] }
    let(:model1_instance) { model1.new(str1: 'A', str2: 'B', id: 99) }
    let(:model1_value) do
      model1_instance.avro_message_value
    end
    let(:model1_key) do
      model1_instance.avro_message_key
    end

    it "returns the associated model for a message" do
      expect(instance.model(model1_key, model1_value)).to equal(model1)
    end

    it_behaves_like "decoding failure cases", :model
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

    it_behaves_like "decoding failure cases", :decode

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

  describe "#decode_hash" do
    let(:models) { [model1, model2] }
    let(:model1_attributes) { { str1: 'A', str2: 'B', id: 99 } }
    let(:model1_instance) { model1.new(model1_attributes) }
    let(:model1_value) do
      model1_instance.avro_message_value
    end
    let(:model1_key) do
      model1_instance.avro_message_key
    end
    let(:model2_attributes) { { id: 100, action: 'CREATE' } }
    let(:model2_value) { model2.new(model2_attributes).avro_message_value }

    it "decodes message value and key pairs to registered models" do
      expect(instance.decode_hash(model1_key, model1_value)).to eq(model1_attributes.stringify_keys)
      expect(instance.decode_hash(model2_value)).to eq(model2_attributes.stringify_keys)
    end

    it_behaves_like "decoding failure cases", :decode_hash
  end
end

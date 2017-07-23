require 'avro/builder'

describe Avromatic::Model::MessagingSerialization do
  let(:values) { { id: rand(99) } }
  let(:instance) { test_class.new(values) }
  let(:avro_message_value) { instance.avro_message_value }
  let(:avro_message_key) { instance.avro_message_key }

  describe "#avro_message_value" do
    let(:test_class) do
      Avromatic::Model.model(value_schema_name: 'test.encode_value')
    end
    let(:values) { { str1: 'a', str2: 'b' } }

    it "encodes the value for the model" do
      message_value = instance.avro_message_value
      decoded = test_class.avro_message_decode(message_value)
      expect(decoded).to eq(instance)
    end

    context "with a nested record" do
      let(:test_class) do
        Avromatic::Model.model(value_schema_name: 'test.nested_record')
      end
      let(:values) { { str: 'a', sub: { str: 'b', i: 1 } } }

      it "encodes the value for the model" do
        expect(instance.sub.str).to eq('b')
        expect(instance.sub.i).to eq(1)
        message_value = instance.avro_message_value
        decoded = test_class.avro_message_decode(message_value)
        expect(decoded).to eq(instance)
      end
    end

    context "with an array of models" do
      let(:test_class) do
        schema = Avro::Builder.build_schema do
          record :key_value do
            required :key, :string
            required :value, :string
          end

          record :key_value_pairs do
            required :id, :int
            required :pairs, :array, items: :key_value
          end
        end
        Avromatic::Model.model(schema: schema)
      end
      let(:values) do
        {
          id: 1,
          pairs: [
            { key: 'foo', value: 'A' },
            { key: 'bar', value: 'B' }
          ]
        }
      end
      let(:instance) { test_class.new(values) }

      before do
        allow(Avromatic.schema_store)
          .to receive(:find).with('key_value_pairs', nil).and_return(test_class.value_avro_schema)
      end

      it "encodes the value for the model" do
        first_pair = instance.pairs.first
        expect(first_pair.key).to eq('foo')
        expect(first_pair.value).to eq('A')
        message_value = instance.avro_message_value
        decoded = test_class.avro_message_decode(message_value)
        expect(decoded).to eq(instance)
      end
    end

    context "with a map of models" do
      let(:test_class) do
        schema = Avro::Builder.build_schema do
          record :submodel do
            required :length, :int
            required :str, :string
          end

          record :with_embedded do
            required :hash, :map, values: :submodel
          end
        end
        Avromatic::Model.model(schema: schema)
      end
      let(:values) do
        {
          hash: {
            foo: { length: 3, str: 'bar' },
            baz: { length: 6, str: 'foobar' }
          }
        }
      end
      let(:instance) { test_class.new(values) }

      before do
        allow(Avromatic.schema_store)
          .to receive(:find).with('with_embedded', nil).and_return(test_class.value_avro_schema)
      end

      it "encodes the value for the model" do
        first_value = instance.hash['foo']
        expect(first_value.length).to eq(3)
        expect(first_value.str).to eq('bar')
        message_value = instance.avro_message_value
        decoded = test_class.avro_message_decode(message_value)
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
      decoded = test_class.avro_message_decode(message_key, message_value)
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

  describe ".avro_message_decode" do
    let(:test_class) do
      Avromatic::Model.model(value_schema_name: 'test.encode_value')
    end
    let(:values) { { str1: 'a', str2: 'b' } }

    it "deserializes a model" do
      decoded = test_class.avro_message_decode(avro_message_value)
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
        decoded = test_class.avro_message_decode(avro_message_key, avro_message_value)
        expect(decoded).to eq(instance)
      end
    end

    context "a model with a union" do
      let(:use_custom_datum_reader) { true }
      let(:schema_name) { 'test.real_union' }
      let(:test_class) do
        Avromatic::Model.model(schema_name: schema_name)
      end
      let(:values) do
        {
          header: 'has bar',
          # This value corresponds to the second union member
          message: { bar_message: "I'm a bar" }
        }
      end
      let(:first_union_member) do
        test_class.attribute_set[:message].type.primitive.types.first
      end

      before do
        instance
        allow(Avromatic).to receive(:use_custom_datum_reader).and_return(use_custom_datum_reader)
        allow(first_union_member).to receive(:new).and_call_original
      end

      it "only coerces using the correct union member" do
        decoded = test_class.avro_message_decode(avro_message_value)
        expect(decoded).to eq(instance)
        expect(first_union_member).not_to have_received(:new)
      end

      context "when use_custom_datum_reader is false" do
        let(:use_custom_datum_reader) { false }

        it "attempts to coerce until a union member matches" do
          decoded = test_class.avro_message_decode(avro_message_value)
          expect(decoded).to eq(instance)
          expect(first_union_member).to have_received(:new)
        end
      end
    end
  end

  describe ".avro_message_attributes" do
    let(:test_class) do
      Avromatic::Model.model(value_schema_name: 'test.encode_value')
    end
    let(:values) { { str1: 'a', str2: 'b' } }
    let(:attributes) { values.stringify_keys }

    it "deserializes attributes for a model" do
      decoded = test_class.avro_message_attributes(avro_message_value)
      expect(decoded).to eq(attributes)
    end

    context "when a value and a key are specified" do
      let(:test_class) do
        Avromatic::Model.model(
          value_schema_name: 'test.encode_value',
          key_schema_name: 'test.encode_key'
        )
      end
      let(:values) { { id: rand(99), str1: 'a', str2: 'b' } }

      it "deserializes attributes for a model" do
        decoded = test_class.avro_message_attributes(avro_message_key, avro_message_value)
        expect(decoded).to eq(attributes)
      end
    end
  end

  it_behaves_like "logical type encoding and decoding" do
    let(:encoded_value) { instance.avro_message_value }
    let(:decoded) { test_class.avro_message_decode(encoded_value) }
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
        decoded = test_class.avro_message_decode(avro_message_value)
        expect(decoded.str).to eq('test')
      end
    end
  end

  describe ".register_schemas!" do
    let(:registry) { Avromatic.build_schema_registry }

    shared_examples_for "value schema registration" do
      it "registers the value schema" do
        expect(test_class.register_schemas!).to be_nil
        registered = registry.subject_version(test_class.value_avro_schema.fullname)
        aggregate_failures do
          expect(registered['version']).to eq(1)
          expect(registered['schema']).to eq(test_class.value_avro_schema.to_s)
        end
      end
    end

    context "a model without a key" do
      let(:test_class) do
        Avromatic::Model.model(value_schema_name: 'test.encode_value')
      end

      it_behaves_like "value schema registration"
    end

    context "a model with a key and value" do
      let(:test_class) do
        Avromatic::Model.model(value_schema_name: 'test.encode_value',
                               key_schema_name: 'test.encode_key')
      end

      it_behaves_like "value schema registration"

      it "registers the key schema" do
        expect(test_class.register_schemas!).to be_nil
        registered = registry.subject_version(test_class.key_avro_schema.fullname)
        aggregate_failures do
          expect(registered['version']).to eq(1)
          expect(registered['schema']).to eq(test_class.key_avro_schema.to_s)
        end
      end
    end
  end
end

require 'spec_helper'
require 'avro/builder'

describe Avromatic::Model::RawSerialization do
  let(:values) { { id: rand(99) } }
  let(:instance) { test_class.new(values) }
  let(:avro_raw_value) { instance.avro_raw_value }
  let(:avro_raw_key) { instance.avro_raw_key }

  before do
    # Ensure that there is no dependency on messaging
    Avromatic.messaging = nil
  end

  describe "#value_attributes_for_avro" do
    let(:test_class) do
      Avromatic::Model.model(value_schema_name: 'test.encode_value')
    end
    let(:values) { { str1: 'a', str2: 'b' } }

    it "returns a hash of attributes that will be encoded using avro" do
      expected = values.stringify_keys
      expect(instance.value_attributes_for_avro).to eq(expected)
    end
  end

  describe "#key_attributes_for_avro" do
    let(:test_class) do
      Avromatic::Model.model(
        value_schema_name: 'test.encode_value',
        key_schema_name: 'test.encode_key'
      )
    end
    let(:values) { super().merge!(str1: 'a', str2: 'b') }

    it "returns a hash of the key attributes that will be encoded using avro" do
      expected = { 'id' => values[:id] }
      expect(instance.key_attributes_for_avro).to eq(expected)
    end
  end

  describe "#avro_raw_value" do
    let(:test_class) do
      Avromatic::Model.model(value_schema_name: 'test.encode_value')
    end
    let(:values) { { str1: 'a', str2: 'b' } }

    it "encodes the value for the model" do
      encoded_value = instance.avro_raw_value
      decoded = test_class.avro_raw_decode(value: encoded_value)
      expect(decoded).to eq(instance)
    end

    context "with a nested record" do
      let(:test_class) do
        Avromatic::Model.model(value_schema_name: 'test.nested_record')
      end
      let(:values) { { str: 'a', sub: { str: 'b', i: 1 } } }

      it "encodes the value for the model" do
        encoded_value = instance.avro_raw_value
        decoded = test_class.avro_raw_decode(value: encoded_value)
        expect(decoded).to eq(instance)
      end
    end
  end

  describe "#avro_raw_key" do
    let(:test_class) do
      Avromatic::Model.model(
        value_schema_name: 'test.encode_value',
        key_schema_name: 'test.encode_key'
      )
    end
    let(:values) { super().merge!(str1: 'a', str2: 'b') }

    it "encodes the key for the model" do
      encoded_value = instance.avro_raw_value
      encoded_key = instance.avro_raw_key
      decoded = test_class.avro_raw_decode(key: encoded_key, value: encoded_value)
      expect(decoded).to eq(instance)
    end

    context "when a model does not have a key schema" do
      let(:test_class) do
        Avromatic::Model.model(value_schema_name: 'test.encode_value')
      end
      let(:values) { { str1: 'a', str2: 'b' } }

      it "raises an error" do
        expect { instance.avro_raw_key }.to raise_error('Model has no key schema')
      end
    end
  end

  describe ".raw_decode" do
    let(:test_class) do
      Avromatic::Model.model(value_schema_name: 'test.encode_value')
    end
    let(:values) { { str1: 'a', str2: 'b' } }

    it "decodes a model" do
      decoded = test_class.avro_raw_decode(value: avro_raw_value)
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

      it "decodes a model" do
        decoded = test_class.avro_raw_decode(key: avro_raw_key, value: avro_raw_value)
        expect(decoded).to eq(instance)
      end

      context "when the writers schemas are different" do
        # schema names for reader and writer must match
        let(:writer_value_schema) do
          Avro::Builder.build_schema do
            record :encode_value, namespace: :test do
              required :str1, :string, default: 'X'
              required :str3, :string, default: 'Z'
            end
          end
        end
        let(:writer_key_schema) do
          Avro::Builder.build_schema do
            record :encode_key, namespace: :test do
              required :id, :int
              required :id_type, :string, default: 'regular'
            end
          end
        end
        let(:writer_test_class) do
          Avromatic::Model.model(value_schema: writer_value_schema,
                                 key_schema: writer_key_schema)
        end
        let(:instance) { writer_test_class.new(values) }
        let(:values) do
          { id: rand(99), id_type: 'admin', str1: 'a', str3: 'c' }
        end

        it "decodes a model based on the writers schema and the model schemas" do
          decoded = test_class.avro_raw_decode(key: avro_raw_key,
                                               value: avro_raw_value,
                                               key_schema: writer_key_schema,
                                               value_schema: writer_value_schema)

          expect(decoded.attributes).to eq(id: values[:id], str1: 'a', str2: 'Y')
        end
      end
    end
  end

  it_behaves_like "logical type encoding and decoding" do
    let(:decoded) { test_class.avro_raw_decode(value: avro_raw_value) }
  end

  context "nested serialization" do
    context "array of array of records" do
      let(:schema) do
        Avro::Builder.build_schema do
          record :int_rec do
            required :i, :int
          end

          record :transform do
            required :matrix, :array, items: array(:int_rec)
          end
        end
      end
      let(:test_class) do
        Avromatic::Model.model(schema: schema)
      end
      let(:values) do
        { matrix: [[{ i: 1 }, { i: 2 }], [{ i: 3 }, { i: 4 }]] }
      end
      let(:decoded) { test_class.avro_raw_decode(value: avro_raw_value) }

      it "encodes and decodes the model" do
        expect(instance).to eq(decoded)
      end
    end

    context "array of pre-registered nested models" do
      let(:nested_schema) do
        Avro::Builder.build_schema do
          record :int_rec do
            required :i, :int
          end
        end
      end
      let!(:nested_model) do
        Avromatic::Model.model(schema: nested_schema)
      end
      let(:schema) do
        Avro::Builder.build_schema do
          record :int_rec do
            required :i, :int
          end

          record :transform do
            required :a, :array, items: :int_rec
          end
        end
      end
      let(:test_class) do
        Avromatic::Model.model(schema: schema)
      end
      let(:values) do
        { a: [{ i: 1 }, { i: 2 }] }
      end
      let(:decoded) { test_class.avro_raw_decode(value: avro_raw_value) }

      it "encodes and decodes the model" do
        expect(instance).to eq(decoded)
      end
    end

    context "array of unions" do
      let(:schema) do
        Avro::Builder.build_schema do
          record :str_rec do
            required :s, :string
          end

          record :int_rec do
            required :i, :int
          end

          record :mgmt do
            required :unions, :array, items: union(:str_rec, :int_rec)
          end
        end
      end
      let(:test_class) do
        Avromatic::Model.model(schema: schema)
      end
      let(:values) do
        { unions: [{ s: 'A' }, { i: 1 }, { s: 'C' }] }
      end
      let(:decoded) { test_class.avro_raw_decode(value: avro_raw_value) }

      it "encodes and decodes the model" do
        expect(instance).to eq(decoded)
      end
    end

    context "map of unions" do
      let(:schema) do
        Avro::Builder.build_schema do
          record :str_rec do
            required :s, :string
          end

          record :int_rec do
            required :i, :int
          end

          record :mgmt do
            required :union_map, :map, values: union(:str_rec, :int_rec)
          end
        end
      end
      let(:test_class) do
        Avromatic::Model.model(schema: schema)
      end
      let(:values) do
        { union_map: {
          'str' => { s: 'A' },
          'int' => { i: 22 }
        } }
      end
      let(:decoded) { test_class.avro_raw_decode(value: avro_raw_value) }

      it "encodes and decodes the model" do
        expect(instance).to eq(decoded)
      end
    end
  end

  context "custom types" do
    let(:schema_name) { 'test.named_type' }
    let(:test_class) do
      Avromatic::Model.model(schema_name: schema_name)
    end
    let(:values) { { six_str: 'fOObAR' } }
    let(:decoded) { test_class.send(:decode_avro_datum, avro_raw_value) }

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

      it "converts when encoding the value" do
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

      it "converts when encoding the value" do
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

      it "converts when encoding the value" do
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

      it "converts when encoding the value" do
        expect(decoded['str']).to eq('length' => 4, 'data' => 'test')
      end
    end
  end

end

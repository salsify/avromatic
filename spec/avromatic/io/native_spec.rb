# frozen_string_literal: true

# TODO: We can probably drop this test now in favor of running the whole test suite with different serialization layers
describe Avromatic::IO::Native do
  let(:value_schema) { true }
  let(:test_class) { Avromatic::Model.model(value_schema_name: schema_name) }
  let(:instance) { test_class.new(attributes) }

  shared_examples_for "a correct encoding roundtrip" do
    it "encodes properly" do
      encoded_value = described_class.encode_model(instance, value_schema)
      expect(encoded_value).to be_a(String)
      decoded = test_class.avro_raw_decode(value: encoded_value)
      expect(decoded).to eq(instance)
    end

    it "decodes properly" do
      encoded_value = instance.avro_raw_value
      expect(encoded_value).to be_a(String)
      decoded_attributes = described_class.decode_attributes(
        encoded_value,
        test_class.value_avro_schema,
        test_class.value_avro_schema,
        false
      )
      decoded = test_class.new(decoded_attributes)
      expect(decoded).to eq(instance)
    end
  end

  context "with a record" do
    let(:schema_name) { 'test.encode_value' }
    let(:attributes) { { str1: 'a', str2: 'b' } }

    it_behaves_like "a correct encoding roundtrip"
  end

  context "primitive types" do
    let(:schema_name) { 'test.primitive_types' }
    let(:attributes) do
      {
        s: 'foo',
        b: '123',
        tf: true,
        i: 777,
        l: 123456789,
        f: 0.5,
        d: 1.0 / 3.0,
        n: nil,
        fx: '1234567',
        e: 'B'
      }
    end

    it_behaves_like "a correct encoding roundtrip"
  end

  context "with a nested record" do
    let(:schema_name) { 'test.nested_record' }
    let(:attributes) { { str: 'a', sub: { str: 'b', i: 1 } } }

    it_behaves_like "a correct encoding roundtrip"
  end

  context "with an array of primitives" do
    let(:schema_name) { 'test.with_array' }
    let(:attributes) { { names: %w[hello world] } }

    it_behaves_like "a correct encoding roundtrip"
  end

  context "with an array of nested record" do
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
    let(:attributes) do
      {
        a: [{ i: 1 }, { i: 2 }]
      }
    end

    it_behaves_like "a correct encoding roundtrip"
  end

  context "with a map of primitives" do
    let(:schema_name) { 'test.with_map' }
    let(:attributes) { { pairs: { a: 1, b: 2 } } }

    it_behaves_like "a correct encoding roundtrip"
  end

  context "with a map of nested record" do
    let(:schema) do
      Avro::Builder.build_schema do
        record :int_rec do
          required :i, :int
        end

        record :transform do
          required :m, :map, values: :int_rec
        end
      end
    end

    let(:test_class) do
      Avromatic::Model.model(schema: schema)
    end

    let(:attributes) do
      {
        m: {
          a: { i: 1 },
          b: { i: 2 }
        }
      }
    end

    it_behaves_like "a correct encoding roundtrip"
  end

  context "with a union of primitives" do
    let(:schema) do
      Avro::Builder.build_schema do
        record :rec do
          required :u1, union(:string, :boolean, :int)
          required :u2, union(:string, :boolean, :int)
          required :u3, union(:string, :boolean, :int)
        end
      end
    end

    let(:test_class) do
      Avromatic::Model.model(schema: schema)
    end

    let(:attributes) do
      {
        u1: 'hello',
        u2: true,
        u3: 7
      }
    end

    it_behaves_like "a correct encoding roundtrip"
  end

  context "with an optional union of primitives" do
    let(:schema) do
      Avro::Builder.build_schema do
        record :rec do
          optional :u1, union(:string, :boolean, :int)
          optional :u2, union(:string, :boolean, :int)
          optional :u3, union(:string, :boolean, :int)
          optional :u4, union(:string, :boolean, :int)
        end
      end
    end

    let(:test_class) do
      Avromatic::Model.model(schema: schema)
    end

    let(:attributes) do
      {
        u1: nil,
        u2: 'hello',
        u3: true,
        u4: 7
      }
    end

    it_behaves_like "a correct encoding roundtrip"
  end

  context "with a union nested records" do
    let(:schema) do
      Avro::Builder.build_schema do
        record :a_rec do
          required :a, :int
        end

        record :b_rec do
          required :b, :int
        end

        record :rec do
          required :u1, union(:a_rec, :b_rec)
          required :u2, union(:a_rec, :b_rec)
        end
      end
    end

    let(:test_class) do
      Avromatic::Model.model(schema: schema)
    end

    let(:attributes) do
      {
        u1: { a: 1 },
        u2: { b: 2 }
      }
    end

    it_behaves_like "a correct encoding roundtrip"
  end

  context "with optional fields" do
    let(:schema) do
      Avro::Builder.build_schema do
        record :int_rec do
          required :i, :int
        end

        record :rec do
          optional :i1, :int_rec
          optional :i2, :int_rec
        end
      end
    end

    let(:test_class) do
      Avromatic::Model.model(schema: schema)
    end

    let(:attributes) do
      {
        i1: { i: 1 },
        i2: nil
      }
    end

    it_behaves_like "a correct encoding roundtrip"
  end

  context "with logical types" do
    let(:schema) do
      Avro::Builder.build_schema do
        record :rec do
          required :date, :int, logical_type: 'date'
          required :ts_msec, :long, logical_type: 'timestamp-millis'
          required :ts_usec, :long, logical_type: 'timestamp-micros'
          required :unknown, :int, logical_type: 'foobar'
        end
      end
    end

    let(:test_class) do
      Avromatic::Model.model(schema: schema)
    end

    let(:attributes) do
      {
        date: Date.today,
        ts_msec: Time.now,
        ts_usec: Time.now,
        unknown: 1
      }
    end

    it_behaves_like "a correct encoding roundtrip"
  end

  context "with custom types that transform on serialization" do
    let(:schema_name) { 'test.named_type' }
    let(:attributes) { { six_str: 'foobar' } }

    before do
      Avromatic.register_type('test.six') do |type|
        type.to_avro = ->(value) { value.upcase }
      end
    end

    it "applies the to_avro callback" do
      encoded_value = described_class.encode_model(instance, true)
      decoded = test_class.avro_raw_decode(value: encoded_value)
      expect(decoded.six_str).to eq('FOOBAR')
    end
  end

  context "with custom types that transform on deserialization" do
    let(:schema_name) { 'test.named_type' }
    let(:attributes) { { six_str: 'foobar' } }

    before do
      Avromatic.register_type('test.six') do |type|
        type.from_avro = ->(value) { value.upcase }
      end
    end

    it "applies the to_avro callback" do
      encoded_value = instance.avro_raw_value
      decoded_attributes = described_class.decode_attributes(
        encoded_value,
        test_class.value_avro_schema,
        test_class.value_avro_schema,
        false
      )
      decoded = test_class.new(decoded_attributes)
      expect(decoded.six_str).to eq('FOOBAR')
    end
  end
end

# frozen_string_literal: true

describe Avromatic::IO::DatumReader do
  let(:test_class) do
    Avromatic::Model.model(schema_name: schema_name)
  end
  let(:instance) { test_class.new(values) }
  let(:avro_message_value) { instance.avro_message_value }
  let(:attributes) { test_class.avro_message_attributes(avro_message_value) }

  describe "#read" do
    let(:decoder) { instance_double(Avro::IO::BinaryDecoder) }

    context "errors" do
      it "raises an error if schemas do not match" do
        reader = described_class.new(Avro::Schema.real_parse('int'), Avro::Schema.real_parse('string'))
        expect { reader.read(decoder) }.to raise_error(Avro::IO::SchemaMatchException)
      end

      context "reader's schema is a union" do
        let(:reader_schema) do
          Avro::Schema.parse([:null, :string].to_json)
        end

        it "raises an error if the writer's schema does not match any reader type" do
          reader = described_class.new(Avro::Schema.real_parse('int'), reader_schema)
          expect { reader.read(decoder) }.to raise_error(Avro::IO::SchemaMatchException)
        end
      end

      it "raises an error when the writer's schema is unknown" do
        writer_schema = Avro::Schema.new('foobar')
        reader = described_class.new(writer_schema, Avro::Schema.real_parse('int'))
        expect { reader.read(decoder) }.to raise_error(Avro::AvroError)
      end
    end
  end

  context "primitive types" do
    let(:schema_name) { 'test.primitive_types' }
    let(:values) do
      {
        s: 'foo',
        b: '123',
        tf: true,
        i: rand(10),
        l: 2 ** 40,
        f: 0.5,
        d: 1.0 / 3.0,
        n: nil,
        fx: '1234567',
        e: 'A'
      }
    end

    # This test case is primarily to provide coverage for DatumReader
    it "reads all primitive types" do
      expect(attributes).to eq(values.stringify_keys)
    end
  end

  context "a record with a union" do
    let(:schema_name) { 'test.real_union' }
    let(:values) do
      {
        header: 'has bar',
        message: { bar_message: "I'm a bar" }
      }
    end

    it "returns a UnionDatum" do
      union_datum = attributes['message']
      expect(union_datum).to be_a_kind_of(Avromatic::IO::UnionDatum)
      expect(union_datum.member_index).to eq(1)
      expect(union_datum.datum).to eq(values[:message].stringify_keys)
    end

    it "can decode a message" do
      expect(test_class.avro_message_decode(avro_message_value)).to eq(instance)
    end

    context "wtih a false value" do
      let(:values) do
        { header: 'has bar', message: false }
      end

      it "can decode a message" do
        expect(test_class.avro_message_decode(avro_message_value)).to eq(instance)
      end
    end

    context "a record with an optional union" do
      let(:schema_name) { 'test.optional_union' }

      it "returns a UnionDatum" do
        union_datum = attributes['message']
        expect(union_datum).to be_a_kind_of(Avromatic::IO::UnionDatum)
        expect(union_datum.member_index).to eq(1)
        expect(union_datum.datum).to eq(values[:message].stringify_keys)
      end

      it "can decode a message" do
        expect(test_class.avro_message_decode(avro_message_value)).to eq(instance)
      end
    end

    context "with an optional field" do
      let(:schema_name) { 'test.with_union' }
      let(:values) { { s: 'foo' } }

      it "does not return a UnionDatum" do
        expect(attributes['s']).not_to be_a_kind_of(Avromatic::IO::UnionDatum)
      end

      it "can decode the message" do
        expect(attributes).to eq(values.stringify_keys)
      end
    end

    context "with null in a union" do
      let(:schema_name) { 'test.null_in_union' }
      let(:values) do
        {
          values: [
            { i: 123 },
            nil,
            { s: 'abc' }
          ]
        }
      end

      it "does not support a null in the middle of a union" do
        expect do
          Avromatic::Model.model(schema_name: schema_name)
        end.to raise_error('a null type in a union must be the first member')
      end
    end

    context "when use_custom_datum_reader is false" do
      before do
        allow(Avromatic).to receive(:use_custom_datum_reader).and_return(false)
      end

      it "does not return a UnionDatum" do
        expect(attributes['message']).not_to be_a_kind_of(Avromatic::IO::UnionDatum)
      end
    end
  end
end

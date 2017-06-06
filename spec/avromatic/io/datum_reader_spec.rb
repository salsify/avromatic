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
        l: 123456789,
        f: 0.5,
        d: 1.0 / 3.0,
        n: nil
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

    it "includes the member index in the decoded hash" do
      expect(attributes['message'][described_class::UNION_MEMBER_INDEX]).to eq(1)
    end

    it "can decode a message" do
      expect(test_class.avro_message_decode(avro_message_value)).to eq(instance)
    end

    context "a record with an optional union" do
      let(:schema_name) { 'test.optional_union' }

      it "includes the member index in the decoded hash" do
        expect(attributes['message'][described_class::UNION_MEMBER_INDEX]).to eq(1)
      end

      it "can decode a message" do
        expect(test_class.avro_message_decode(avro_message_value)).to eq(instance)
      end
    end

    context "with an optional field" do
      let(:schema_name) { 'test.with_union' }
      let(:values) { { s: 'foo' } }

      it "does not include a member index in the decoded hash" do
        expect(attributes).not_to have_key(described_class::UNION_MEMBER_INDEX)
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

      it "includes the member index in the decoded hash" do
        pending "a null type in the middle of union member types is currently broken/unsupported"

        expect(attributes['values'][0][described_class::UNION_MEMBER_INDEX]).to eq(0)
        expect(attributes['values'][2][described_class::UNION_MEMBER_INDEX]).to eq(2)
      end

      it "can decode a message" do
        pending "a null type in the middle of union member types is currently broken/unsupported"

        expect(test_class.avro_message_decode(avro_message_value)).to eq(instance)
      end
    end

    context "when use_custom_datum_reader is false" do
      before do
        allow(Avromatic).to receive(:use_custom_datum_reader).and_return(false)
      end

      it "does not include the member index in the decoded hash" do
        expect(attributes['message']).not_to have_key(described_class::UNION_MEMBER_INDEX)
      end
    end
  end
end

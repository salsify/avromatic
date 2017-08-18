describe Avromatic::IO::DatumWriter do
  let(:encoder) { instance_double(Avro::IO::BinaryEncoder) }
  let(:schema_name) { 'test.real_union' }
  let(:test_class) do
    Avromatic::Model.model(schema_name: schema_name)
  end
  let(:values) do
    {
      headers: 'has bar',
      message: { bar_message: 'bar' }
    }
  end
  let(:instance) { test_class.new(values) }
  let(:use_custom_datum_writer) { true }
  let(:datum_writer) { described_class.new(test_class.value_avro_schema) }
  let(:union_schema) do
    test_class.value_avro_schema.fields.find { |f| f.name == 'message' }.type
  end
  let(:datum) { instance.value_attributes_for_avro['message'] }

  before do
    allow(Avromatic).to receive(:use_custom_datum_writer).and_return(use_custom_datum_writer)
    allow(Avro::Schema).to receive(:validate).and_call_original
    allow(encoder).to receive(:write_long)
    allow(datum_writer).to receive(:write_data)
  end

  describe "#write_union" do
    before { datum_writer.write_union(union_schema, datum, encoder) }

    context "when the datum includes union member index" do
      it "does not call Avro::Schema.validate" do
        expect(Avro::Schema).not_to have_received(:validate)
      end

      it "calls write_data to encode the union" do
        expect(datum_writer).to have_received(:write_data).with(union_schema.schemas[1], datum, encoder)
      end
    end

    context "when the datum does not include union member index" do
      let(:use_custom_datum_writer) { false }

      it "calls validate to find the matching schema" do
        expect(Avro::Schema).to have_received(:validate).twice
      end

      it "calls write_data to encode the union" do
        expect(datum_writer).to have_received(:write_data).with(union_schema.schemas[1], datum, encoder)
      end
    end
  end

  describe "#write_record" do
    let(:message) { datum }
    let(:pre_encoded) { 'foo' }

    before do
      allow(message).to receive(:avro_raw_value).and_return(pre_encoded)
      allow(encoder).to receive(:write)
      datum_writer.write_record(union_schema.schemas[1], datum, encoder)
    end

    context "when the datum includes an encoding provider" do
      let(:message) { datum[Avromatic::IO::ENCODING_PROVIDER] }

      it "delegates encoding to the model" do
        expect(encoder).to have_received(:write).with(pre_encoded)
      end
    end

    context "when the datum doesn't include an encoding provider" do
      let(:use_custom_datum_writer) { false }

      it "doesn't delegate encoding to the model" do
        union_schema.schemas[1].fields.each do |field|
          expect(datum_writer).to have_received(:write_data).with(field.type, datum[field.name], encoder)
        end
      end
    end
  end
end

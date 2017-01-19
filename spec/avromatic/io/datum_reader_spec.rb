describe Avromatic::IO::DatumReader do
  let(:test_class) do
    Avromatic::Model.model(schema_name: schema_name)
  end
  let(:instance) { test_class.new(values) }
  let(:avro_message_value) { instance.avro_message_value }
  let(:attributes) { test_class.avro_message_attributes(avro_message_value) }

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

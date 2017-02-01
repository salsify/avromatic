# rubocop:disable RSpec/FilePath

describe AvroTurf::SchemaRegistry, 'schema registry patch' do
  let(:logger) { Logger.new(StringIO.new) }
  let(:subject_name) { 'some-subject' }
  let(:schema) do
    {
      type: 'record',
      name: 'person',
      fields: [
        { name: 'name', type: 'string' }
      ]
    }.to_json
  end
  let(:avro_schema) { Avro::Schema.parse(schema) }
  let(:registry) { described_class.new(Avromatic.registry_url, logger: logger) }

  describe "#register" do
    it "allows registration of an Avro JSON schema" do
      id = registry.register(subject_name, schema)
      expect(registry.fetch(id)).to eq(avro_schema.to_s)
    end

    it "allows the registration of an Avro::Schema" do
      id = registry.register(subject_name, avro_schema)
      expect(registry.fetch(id)).to eq(avro_schema.to_s)
    end

    it "makes a request to check if the schema exists before attempting to register" do
      id = registry.register(subject_name, avro_schema)
      allow(registry).to receive(:post)
      expect(registry.register(subject_name, avro_schema)).to eq(id)
      expect(registry).not_to have_received(:post)
    end

    context "when use_cacheable_schema_registration is false" do
      before do
        allow(Avromatic).to receive(:use_cacheable_schema_registration).and_return(false)
      end

      it "does not check that the schema exists before attempting to register" do
        allow(registry).to receive(:get).and_call_original
        registry.register(subject_name, avro_schema)
        expect(registry).not_to have_received(:get)
      end
    end

    context "when the check prior to registration raises an error other than NotFound" do
      before do
        allow(registry).to receive(:get).and_raise(Excon::Errors::InternalServerError.new('error'))
      end

      it "raises the error" do
        expect do
          registry.register(subject_name, schema)
        end.to raise_error(Excon::Errors::InternalServerError)
      end
    end
  end

  describe "#lookup_subject_schema" do
    context "when the schema does not exist" do
      it "raises an error" do
        expect do
          registry.lookup_subject_schema(subject_name, schema)
        end.to raise_error(Excon::Errors::NotFound)
      end
    end

    context "with a previously registered schema" do
      let!(:id) { registry.register(subject_name, schema) }

      it "allows lookup using an Avro JSON schema" do
        expect(registry.lookup_subject_schema(subject_name, schema)).to eq(id)
      end

      it "allows lookup using an Avro::Schema object" do
        expect(registry.lookup_subject_schema(subject_name, avro_schema)).to eq(id)
      end
    end
  end
end

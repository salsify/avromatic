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
      allow(FakeSchemaRegistryServer).to receive(:post)
      expect(registry.register(subject_name, avro_schema)).to eq(id)
      expect(FakeSchemaRegistryServer).not_to have_received(:post)
    end
  end
end

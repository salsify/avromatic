require 'avro_turf'
require 'avro_turf/schema_registry'

AvroTurf::SchemaRegistry.class_eval do
  # Override register to first check if a schema is registered by fingerprint
  def register(subject, schema)
    raise 'schema must be an Avro::Schema' unless schema.is_a?(Avro::Schema)

    registered = false
    data = begin
             get("/subjects/#{subject}/fingerprints/#{schema.sha256_fingerprint.to_s(16)}")
           rescue
             registered = true
             post("/subjects/#{subject}/versions", body: { schema: schema.to_s }.to_json)
           end

    id = data.fetch('id')

    @logger.info("#{registered ? 'Registered' : 'Found'} schema for subject `#{subject}`; id = #{id}")

    id
  end
end

require 'avro_turf'
require 'avro_turf/schema_registry'

AvroTurf::SchemaRegistry.class_eval do
  # Override register to first check if a schema is registered by fingerprint
  def register(subject, schema)
    schema_object = if schema.is_a?(String)
                      Avro::Schema.parse(schema)
                    else
                      schema
                    end

    registered = false
    data = begin
             get("/subjects/#{subject}/fingerprints/#{schema_object.sha256_fingerprint.to_s(16)}")
           rescue
             registered = true
             post("/subjects/#{subject}/versions", body: { schema: schema.to_s }.to_json)
           end

    id = data.fetch('id')

    @logger.info("#{registered ? 'Registered' : 'Found'} schema for subject `#{subject}`; id = #{id}")

    id
  end
end

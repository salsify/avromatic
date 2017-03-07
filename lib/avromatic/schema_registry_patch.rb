require 'avro_turf'
require 'avro_turf/confluent_schema_registry'

module Avromatic
  module CacheableSchemaRegistration
    # Override register to first check if a schema is registered by fingerprint
    def register(subject, schema)
      return super unless Avromatic.use_cacheable_schema_registration

      begin
        lookup_subject_schema(subject, schema)
      rescue Excon::Errors::NotFound
        data = post("/subjects/#{subject}/versions", body: { schema: schema.to_s }.to_json)
        id = data.fetch('id')
        @logger.info("Registered schema for subject `#{subject}`; id = #{id}")
        id
      end
    end

    def lookup_subject_schema(subject, schema)
      schema_object = if schema.is_a?(String)
                        Avro::Schema.parse(schema)
                      else
                        schema
                      end

      data = get("/subjects/#{subject}/fingerprints/#{schema_object.sha256_fingerprint.to_s(16)}")
      id = data.fetch('id')
      @logger.info("Found schema for subject `#{subject}`; id = #{id}")
      id
    end
  end
end

AvroTurf::ConfluentSchemaRegistry.prepend(Avromatic::CacheableSchemaRegistration)

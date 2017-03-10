require 'avro_turf/test/fake_confluent_schema_registry_server'
require 'avro-resolution_canonical_form'

# Add support for endpoint to lookup subject schema id by fingerprint.
FakeConfluentSchemaRegistryServer.class_eval do
  SCHEMA_NOT_FOUND = FakeConfluentSchemaRegistryServer::SCHEMA_NOT_FOUND
  SCHEMAS = FakeConfluentSchemaRegistryServer::SCHEMAS
  SUBJECTS = FakeConfluentSchemaRegistryServer::SUBJECTS

  get '/subjects/:subject/fingerprints/:fingerprint' do
    subject = params[:subject]
    halt(404, SCHEMA_NOT_FOUND) unless SUBJECTS.key?(subject)

    fingerprint = params[:fingerprint]
    fingerprint = fingerprint.to_i.to_s(16) if /^\d+$/ =~ fingerprint

    schema_id = SCHEMAS.find_index do |schema|
      Avro::Schema.parse(schema).sha256_resolution_fingerprint.to_s(16) == fingerprint
    end

    halt(404, SCHEMA_NOT_FOUND) unless schema_id && SUBJECTS[subject].include?(schema_id)

    { id: schema_id }.to_json
  end
end

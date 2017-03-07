require 'avromatic/test/fake_confluent_schema_registry_server'
require 'avro_turf/test/fake_schema_registry_server'

# This file is for back compatibility. The class has been renamed
# FakeConfluentSchemaRegistryServer, but avro_turf defines an alias for the
# old name, so we alias the require for now.

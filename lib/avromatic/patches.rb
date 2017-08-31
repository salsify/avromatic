if defined?(AvroPatches)
  require 'avromatic/patches/schema_validator_patch'
  Avro::SchemaValidator.singleton_class.prepend(Avromatic::Patches::SchemaValidatorPatch)
end

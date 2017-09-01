loaded_avro_patches = begin
    require 'avro-patches'
    true
  rescue LoadError
    false
  end

if loaded_avro_patches
  require 'avromatic/patches/schema_validator_patch'
  Avro::SchemaValidator.singleton_class.prepend(Avromatic::Patches::SchemaValidatorPatch)
end

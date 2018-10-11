# frozen_string_literal: true

loaded_avro_patches = begin
    require 'avro-patches'
    true
  rescue LoadError
    false
  end

if loaded_avro_patches
  require 'avromatic/patches/schema_validator_patch'
  avro_patches_version = Gem::Version.new(AvroPatches::VERSION)
  if avro_patches_version < Gem::Version.new('0.4.0')
    Avro::SchemaValidator.singleton_class.prepend(Avromatic::Patches::SchemaValidatorPatch)
  else
    Avro::SchemaValidator.singleton_class.prepend(Avromatic::Patches::SchemaValidatorPatchV040)
  end
end

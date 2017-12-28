# avromatic changelog

## v0.29.1
- Add `Avromatic.build_messaging!` to `avromatic/rspec`.

## v0.29.0
- Add new public methods `#avro_key_datum` and `#avro_value_datum` on an
  Avromatic model instance that return the attributes of the model suitable for
  Avro encoding without any customizations.

## v0.28.1
- Fix a bug that raised an error when encoding a cached model containing optional
  field(s). With this change, immutable model caching now enabled only when 
  `avro-patches` is present.

## v0.28.0
- Add support for caching avro encodings for immutable models

## v0.27.0
- Patches avromatic model classes to cache `Virtus::ValueObject::AllowedWriterMethods#allowed_writer_methods`
- Support Rails 5.1

## v0.26.0
- Caches result of Avromatic::Model::RawSerialization#value_attributes_for_avro for immutable models

## v0.25.0
- Disallow optional fields in schemas used for keys by default.

## v0.24.0
- Add `Avromatic::IO::DatumWriter` to optimize the encoding of Avro unions
  from Avromatic models.
- Expose the `#key_attributes_for_avro` method on models.

## v0.23.0
- Add mutable option when defining a model.

## v0.22.0
- Require `avro_schema_registry_client` v0.3.0 or later to avoid
  using `avro-salsify-fork`.

## v0.21.1
- Fix a bug in the optimization of optional union decoding.

## v0.21.0
- Remove monkey-patches for `AvroTurf::ConfluentSchemaRegistry` and
  `FakeConfluentSchemaRegistryServer` and depend on `avro_schema_registry-client`
  instead.
- Rename the configuration option `use_cacheable_schema_registration` to
  `use_schema_fingerprint_lookup`.

## v0.20.0
- Support schema stores with a `#clear_schemas` method.

## v0.19.0
- Use a fingerprint based on `avro-resolution_canonical_form`.

## v0.18.1
- Replace another reference to `avromatic/test/fake_schema_registry_server`.

## v0.18.0
- Compatibility with `avro_turf` v0.8.0. `avromatic/test/fake_schema_registry_server`
  is now deprecated and will be removed in a future release.
  Use `avromatic/test/fake_confluent_schema_registry_server` instead.

## v0.17.1
- Correctly namespace Avro errors raised by `Avromatic::IO::DatumReader`.

## v0.17.0
- Add `.register_schemas!` method to generated models to register the associated
  schemas in a schema registry.

## v0.16.0
- Add `#lookup_subject_schema` method to `AvroTurf::SchemaRegistry` patch to
  directly support looking up existing schema ids by fingerprint.

## v0.15.1
- Add `Avromatic.use_cacheable_schema_registration` option to control the lookup
  of existing schema ids by fingerprint.

## v0.15.0
- Add patch to `AvroTurf::SchemaRegistry` to lookup existing schema ids using
  `GET /subjects/:subject/fingerprints/:fingerprint` from `#register`.
  This endpoint is supported in the avro-schema-registry.
- Add patch to the `FakeSchemaRegistryServer` from `AvroTurf` to support the
  fingerprint endpoint.

## v0.14.0
- Add `Avromatic::Messaging` and `Avromatic::IO::DatumReader` classes to
  optimize the decoding of Avro unions to Avromatic models.

## v0.13.0
- Add interfaces to deserialize as a hash of attributes instead of a model.

## v0.12.0
- Clear the schema store, if it supports it, prior to code reloading in Rails
  applications. This allows schema changes to be picked up during code
  reloading.

## v0.11.2
- Fix for models containing optional array and map fields.

## v0.11.1
- Another fix for Rails initialization and reloading. Do not clear the nested
  models registry the first time that the `to_prepare` hook is called.

## v0.11.0
- Replace `Avromatic.on_initialize` proc with `Avromatic.eager_load_models`
  array. The models listed by this configuration are added to the registry
  at the end of `.configure` and prior to code reloading in Rails applications.
  This is a compatibility breaking change.

## v0.10.0
- Add `Avromatic.on_initialize` proc that is called at the end of `.configure`
  and on code reloading in Rails applications.

## v0.9.0
- Experimental: Add support for more than one non-null type in a union.
- Allow nested models to be referenced and reused.
- Fix the serialization of nested complex types.
- Add support for recursive models.
- Allow required array and map fields to be empty. Only nil values for required
  array and map fields are now considered invalid.
- Validate nested models. This includes models embedded within other complex
  types (array, map, and union).
- Truncate values for timestamps to the precision supported by the logical type.

## v0.8.0
- Add support for logical types. Currently this requires using the
  `avro-salsify-fork` gem for logical types support with Ruby.

## v0.7.1
- Raise a more descriptive error when attempting to generate a model for a
  non-record Avro type.

## v0.7.0
- Add RSpec `FakeSchemaRegistryServer` test helper.

## v0.6.2
- Remove dependency on `Hash#transform_values` from `ActiveSupport` v4.2.

## v0.6.1
- Fix serialization of array and map types that contain nested models.

## v0.6.0
- Require `avro_turf` v0.7.0 or later.

## v0.5.0
- Rename `Avromatic::Model::Decoder` to `MessageDecoder`.
- Rename `.deserialize` on generated models to `.avro_message_decode`.
- Add `#avro_raw_key`, `#avro_raw_value` and `.avro_raw_decode` methods to
  generated models to support encoding and decoding without a schema registry.

## v0.4.0
- Allow the specification of a custom type, including conversion to/from Avro,
  for named types.

## v0.3.0
- Remove dependency on the `private_attr` gem.

## v0.2.0
- Allow a module level schema registry to be configured.

## v0.1.2
- Do not build an `AvroTurf::Messaging` object for each model class.

## v0.1.1
- Fix Railtie.

## v0.1.0
- Initial release

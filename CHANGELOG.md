# avromatic changelog

## v0.9.0 (unreleased)
- Experimental: Add support for more than one non-null type in a union.
- Allow nested models to be referenced and reused.
- Fix the serialization of nested complex types.
- Add support for recursive models.

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

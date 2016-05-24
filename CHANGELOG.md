# avromatic changelog

## v0.5.0
- Add `#avro_encoded_key`, `#avro_encoded_value` and `.decode` methods to
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

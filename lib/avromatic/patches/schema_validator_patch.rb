module Avromatic
  module Patches
    module SchemaValidatorPatch
      # This method replaces validate_recursive in AvroPatches::LogicalTypes::SchemaValidatorPatch
      # to enable validating datums that contain an encoding provider.
      def validate_recursive(expected_schema, logical_datum, path, result, encoded = false)
        datum = resolve_datum(expected_schema, logical_datum, encoded)
        case expected_schema.type_sym
        when :record, :error, :request
          if datum.is_a?(Hash) && datum.key?(Avromatic::IO::ENCODING_PROVIDER)
            return if expected_schema.sha256_resolution_fingerprint ==
              datum[Avromatic::IO::ENCODING_PROVIDER].value_avro_schema.sha256_resolution_fingerprint
            raise Avro::SchemaValidator::ValidationError
          end
        end
        super(expected_schema, logical_datum, path, result, encoded)
      end
    end
  end
end

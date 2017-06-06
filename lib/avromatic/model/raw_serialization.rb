require 'avromatic/model/passthrough_serializer'

module Avromatic
  module Model

    # This module provides serialization support for encoding directly to Avro
    # without dependency on a schema registry.
    module RawSerialization
      extend ActiveSupport::Concern

      module Encode
        extend ActiveSupport::Concern

        delegate :avro_serializer, :datum_writer, :datum_reader, :attribute_set,
                 to: :class
        private :avro_serializer, :datum_writer, :datum_reader

        module ClassMethods
          def recursive_serialize(value, attribute_name = nil)
            if value.is_a?(Avromatic::Model::Attributes)
              value.value_attributes_for_avro
            elsif value.is_a?(Array)
              value.map { |v| recursive_serialize(v) }
            elsif value.is_a?(Hash)
              value.each_with_object({}) do |(k, v), hash|
                hash[k] = recursive_serialize(v)
              end
            else
              avro_serializer[attribute_name].call(value)
            end
          end
        end

        def avro_raw_value
          avro_raw_encode(value_attributes_for_avro, :value)
        end

        def avro_raw_key
          raise 'Model has no key schema' unless key_avro_schema
          avro_raw_encode(key_attributes_for_avro, :key)
        end

        def value_attributes_for_avro
          avro_hash(value_avro_field_names)
        end

        def key_attributes_for_avro
          avro_hash(key_avro_field_names)
        end

        private

        def avro_hash(fields)
          attributes.slice(*fields).each_with_object(Hash.new) do |(key, value), result|
            result[key.to_s] = self.class.recursive_serialize(value, key)
          end
        end

        def avro_raw_encode(data, key_or_value = :value)
          stream = StringIO.new
          encoder = Avro::IO::BinaryEncoder.new(stream)
          datum_writer[key_or_value].write(data, encoder)
          stream.string
        end
      end
      include Encode

      module Decode

        # Create a new instance based on an encoded value and optional encoded key.
        # If supplied then the key_schema and value_schema are used as the writer's
        # schema for the corresponding value. The model's schemas are always used
        # as the reader's schemas.
        def avro_raw_decode(key: nil, value:, key_schema: nil, value_schema: nil)
          key_attributes = key && decode_avro_datum(key, key_schema, :key)
          value_attributes = decode_avro_datum(value, value_schema, :value)

          new(value_attributes.merge!(key_attributes || {}))
        end

        private

        def decode_avro_datum(data, schema = nil, key_or_value = :value)
          stream = StringIO.new(data)
          decoder = Avro::IO::BinaryDecoder.new(stream)
          reader = schema ? custom_datum_reader(schema, key_or_value) : datum_reader[key_or_value]
          reader.read(decoder)
        end

        def custom_datum_reader(schema, key_or_value)
          datum_reader_class.new(schema, send("#{key_or_value}_avro_schema"))
        end
      end

      module ClassMethods
        def datum_reader_class
          Avromatic::IO::DatumReader
        end

        # Store a hash of Procs by field name (as a symbol) to convert
        # the value before Avro serialization.
        # Returns the default PassthroughSerializer if a key is not present.
        def avro_serializer
          @avro_serializer ||= Hash.new(PassthroughSerializer)
        end

        def datum_writer
          @datum_writer ||= begin
                              hash = { value: Avro::IO::DatumWriter.new(value_avro_schema) }
                              hash[:key] = Avro::IO::DatumWriter.new(key_avro_schema) if key_avro_schema
                              hash
                            end
        end

        def datum_reader
          @datum_reader ||= begin
            hash = { value: datum_reader_class.new(value_avro_schema) }
            hash[:key] = datum_reader_class.new(key_avro_schema) if key_avro_schema
            hash
          end
        end

        include Decode
      end
    end
  end
end

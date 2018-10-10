# frozen_string_literal: true

module Avromatic
  module Model

    # This module provides serialization support for encoding directly to Avro
    # without dependency on a schema registry.
    module RawSerialization
      extend ActiveSupport::Concern

      module Encode
        extend ActiveSupport::Concern

        delegate :datum_writer, :datum_reader, to: :class
        private :datum_writer, :datum_reader

        def avro_raw_value
          if self.class.config.mutable
            avro_raw_encode(value_attributes_for_avro, :value)
          else
            @avro_raw_value ||= avro_raw_encode(value_attributes_for_avro, :value)
          end
        end

        def avro_raw_key
          raise 'Model has no key schema' unless key_avro_schema
          avro_raw_encode(key_attributes_for_avro, :key)
        end

        def value_attributes_for_avro
          if self.class.config.mutable
            avro_hash(value_avro_field_names)
          else
            @value_attributes_for_avro ||= avro_hash(value_avro_field_names)
          end
        end

        def key_attributes_for_avro
          avro_hash(key_avro_field_names)
        end

        def avro_value_datum
          if self.class.config.mutable
            avro_hash(value_avro_field_names, strict: true)
          else
            @avro_datum ||= avro_hash(value_avro_field_names, strict: true)
          end
        end

        def avro_key_datum
          avro_hash(key_avro_field_names, strict: true)
        end

        private

        def avro_hash(fields, strict: false)
          attributes.slice(*fields).each_with_object(Hash.new) do |(key, value), result|
            result[key.to_s] = attribute_definitions[key].serialize(value, strict: strict)
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
          Avromatic.use_custom_datum_reader ? Avromatic::IO::DatumReader : Avro::IO::DatumReader
        end

        def datum_writer_class
          Avromatic.use_custom_datum_writer ? Avromatic::IO::DatumWriter : Avro::IO::DatumWriter
        end

        def datum_writer
          @datum_writer ||= begin
                              hash = { value: datum_writer_class.new(value_avro_schema) }
                              hash[:key] = datum_writer_class.new(key_avro_schema) if key_avro_schema
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

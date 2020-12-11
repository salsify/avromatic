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

        def avro_raw_value(validate: true)
          if self.class.config.mutable
            avro_raw_encode(value_attributes_for_avro(validate: validate), :value)
          else
            @avro_raw_value ||= avro_raw_encode(value_attributes_for_avro(validate: validate), :value)
          end
        end

        def avro_raw_key(validate: true)
          raise 'Model has no key schema' unless key_avro_schema
          avro_raw_encode(key_attributes_for_avro(validate: validate), :key)
        end

        def value_attributes_for_avro(validate: true)
          if self.class.config.mutable
            avro_hash(value_avro_field_references, validate: validate)
          else
            @value_attributes_for_avro ||= avro_hash(value_avro_field_references, validate: validate)
          end
        end

        def key_attributes_for_avro(validate: true)
          avro_hash(key_avro_field_references, validate: validate)
        end

        def avro_value_datum(validate: true)
          if self.class.config.mutable
            avro_hash(value_avro_field_references, strict: true, validate: validate)
          else
            @avro_datum ||= avro_hash(value_avro_field_references, strict: true, validate: validate)
          end
        end

        def avro_key_datum(validate: true)
          avro_hash(key_avro_field_references, strict: true, validate: validate)
        end

        private

        def avro_hash(field_references, strict: false, validate:)
          avro_validate! if validate
          field_references.each_with_object(Hash.new) do |field_reference, result|
            next unless _attributes.include?(field_reference.name_sym)

            value = _attributes[field_reference.name_sym]
            result[field_reference.name] = attribute_definitions[field_reference.name_sym].serialize(value, strict)
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
          value_attributes.merge!(key_attributes) if key_attributes
          new(value_attributes)
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

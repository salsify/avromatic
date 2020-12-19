# frozen_string_literal: true

require_relative 'attribute_path'
require 'active_support/deprecation'

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

        UNSPECIFIED = Object.new

        def avro_raw_value(validate: UNSPECIFIED)
          unless validate == UNSPECIFIED
            ActiveSupport::Deprecation.warn("The 'validate' argument to #{__method__} is deprecated.")
          end

          if self.class.config.mutable
            avro_raw_encode(value_attributes_for_avro, :value)
          else
            @avro_raw_value ||= avro_raw_encode(value_attributes_for_avro, :value)
          end
        end

        def avro_raw_key(validate: UNSPECIFIED)
          unless validate == UNSPECIFIED
            ActiveSupport::Deprecation.warn("The 'validate' argument to #{__method__} is deprecated.")
          end

          raise 'Model has no key schema' unless key_avro_schema
          avro_raw_encode(key_attributes_for_avro, :key)
        end

        def value_attributes_for_avro(validate: UNSPECIFIED)
          unless validate == UNSPECIFIED
            ActiveSupport::Deprecation.warn("The 'validate' argument to #{__method__} is deprecated.")
          end

          if self.class.config.mutable
            avro_hash(value_avro_field_references)
          else
            @value_attributes_for_avro ||= avro_hash(value_avro_field_references)
          end
        end

        def key_attributes_for_avro(validate: UNSPECIFIED)
          unless validate == UNSPECIFIED
            ActiveSupport::Deprecation.warn("The 'validate' argument to #{__method__} is deprecated.")
          end

          avro_hash(key_avro_field_references)
        end

        def avro_value_datum(validate: UNSPECIFIED)
          unless validate == UNSPECIFIED
            ActiveSupport::Deprecation.warn("The 'validate' argument to #{__method__} is deprecated.")
          end

          if self.class.config.mutable
            avro_hash(value_avro_field_references, strict: true)
          else
            @avro_datum ||= avro_hash(value_avro_field_references, strict: true)
          end
        end

        def avro_key_datum(validate: UNSPECIFIED)
          unless validate == UNSPECIFIED
            ActiveSupport::Deprecation.warn("The 'validate' argument to #{__method__} is deprecated.")
          end

          avro_hash(key_avro_field_references, strict: true)
        end

        private

        def avro_hash(field_references, strict: false)
          missing_attributes = nil
          avro_hash = field_references.each_with_object(Hash.new) do |field_reference, result|
            attribute_definition = self.class.attribute_definitions[field_reference.name_sym]
            value = _attributes[field_reference.name_sym]

            if value.nil? && !attribute_definition.nullable?
              missing_attributes ||= []
              missing_attributes << Avromatic::Model::AttributePath.new(field_reference.name)
            end

            next unless _attributes.include?(field_reference.name_sym)

            begin
              result[field_reference.name] = attribute_definition.serialize(value, strict)
            rescue Avromatic::Model::ValidationError => e
              missing_attributes ||= []
              e.missing_attributes.each do |nested_attribute|
                missing_attributes << nested_attribute.prepend_attribute_access(field_reference.name)
              end
            end
          end

          if missing_attributes.present?
            message = "#{self.class.name}(#{_attributes.inspect}) cannot be serialized because the following " \
              "attributes are nil: #{missing_attributes.map(&:to_s).join(', ')}"
            raise Avromatic::Model::ValidationError.new(message, missing_attributes)
          else
            avro_hash
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

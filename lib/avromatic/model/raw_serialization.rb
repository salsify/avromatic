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

        EMPTY_ARRAY = [].freeze

        included do
          @attribute_member_types = {}
        end

        module ClassMethods
          def recursive_serialize(value, name: nil, member_types: nil, strict: false) # rubocop:disable Lint/ShadowedArgument
            member_types = attribute_member_types(name) if name
            member_types ||= EMPTY_ARRAY

            if value.is_a?(Avromatic::Model::Attributes)
              hash = strict ? value.avro_value_datum : value.value_attributes_for_avro
              if !strict && Avromatic.use_custom_datum_writer
                if Avromatic.use_encoding_providers? && !value.class.config.mutable
                  # n.b. Ideally we'd just return value here instead of wrapping it in a
                  # hash but then we'd have no place to stash the union member index...
                  hash = { Avromatic::IO::ENCODING_PROVIDER => value }
                end
                member_index = member_types.index(value.class) if member_types.any?
                hash[Avromatic::IO::UNION_MEMBER_INDEX] = member_index if member_index
              end
              hash
            elsif value.is_a?(Array)
              value.map { |v| recursive_serialize(v, member_types: member_types, strict: strict) }
            elsif value.is_a?(Hash)
              value.each_with_object({}) do |(k, v), map|
                map[k] = recursive_serialize(v, member_types: member_types, strict: strict)
              end
            else
              avro_serializer[name].call(value)
            end
          end

          private

          def attribute_member_types(name)
            @attribute_member_types.fetch(name) do
              member_types = nil
              attribute = attribute_set[name] if name
              if attribute
                if attribute.primitive == Array &&
                  attribute.member_type.is_a?(Avromatic::Model::Attribute::Union)
                  member_types = attribute.member_type.primitive.types
                elsif attribute.primitive == Hash &&
                  attribute.value_type.is_a?(Avromatic::Model::Attribute::Union)
                  member_types = attribute.value_type.primitive.types
                elsif attribute.options[:primitive] == Avromatic::Model::AttributeType::Union
                  member_types = attribute.primitive.types
                end
              end
              @attribute_member_types[name] = member_types
            end
          end
        end

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
            result[key.to_s] = self.class.recursive_serialize(value, name: key, strict: strict)
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

        # Store a hash of Procs by field name (as a symbol) to convert
        # the value before Avro serialization.
        # Returns the default PassthroughSerializer if a key is not present.
        def avro_serializer
          @avro_serializer ||= Hash.new(PassthroughSerializer)
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

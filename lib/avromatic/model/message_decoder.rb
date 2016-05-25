require 'avro_turf/schema_registry'

module Avromatic
  module Model

    # This class is used to decode messages encoded using Avro to their
    # corresponding models.
    class MessageDecoder
      MAGIC_BYTE = [0].pack('C').freeze

      class UnexpectedKeyError < StandardError
        def initialize(schema_key)
          super("Unexpected schemas #{schema_key}")
        end
      end

      class MagicByteError < StandardError
        def initialize(magic_byte)
          super("Expected data to begin with a magic byte, got '#{magic_byte}'")
        end
      end

      class DuplicateKeyError < StandardError
        def initialize(*models)
          super("Multiple models #{models} have the same key "\
                "'#{Avromatic::Model::MessageDecoder.model_key(models.first)}'")
        end
      end

      def self.model_key(model)
        [model.key_avro_schema && model.key_avro_schema.fullname,
         model.value_avro_schema.fullname]
      end

      delegate :model_key, to: :class

      # @param *models [generated models] Models to register for decoding.
      # @param schema_registry [Avromatic::SchemaRegistryClient] Optional schema
      #   registry client.
      # @param registry_url [String] Optional URL for schema registry server.
      def initialize(*models, schema_registry: nil, registry_url: nil)
        @model_map = build_model_map(models)
        @schema_names_by_id = {}
        @schema_registry = schema_registry ||
          Avromatic.schema_registry ||
          (registry_url && AvroTurf::SchemaRegistry.new(registry_url, logger: Avromatic.logger)) ||
          Avromatic.build_schema_registry
      end

      # If two arguments are specified then the first is interpreted as the
      # message key and the second is the message value. If there is only one
      # arg then it is used as the message value.
      # @return [Avromatic model]
      def decode(*args)
        message_key, message_value = args.size > 1 ? args : [nil, args.first]
        value_schema_name = schema_name_for_data(message_value)
        key_schema_name = schema_name_for_data(message_key) if message_key
        deserialize([key_schema_name, value_schema_name], message_key, message_value)
      end

      private

      attr_reader :schema_names_by_id, :model_map, :schema_registry

      def deserialize(model_key, message_key, message_value)
        raise UnexpectedKeyError.new(model_key) unless model_map.key?(model_key)
        model_map[model_key].avro_message_decode(message_key, message_value)
      end

      def schema_name_for_data(data)
        validate_magic_byte!(data)
        schema_id = extract_schema_id(data)
        lookup_schema_name(schema_id)
      end

      def lookup_schema_name(schema_id)
        schema_names_by_id.fetch(schema_id) do
          schema = Avro::Schema.parse(schema_registry.fetch(schema_id))
          schema_names_by_id[schema_id] = schema.fullname
        end
      end

      def extract_schema_id(data)
        data[1..4].unpack('N').first
      end

      def validate_magic_byte!(data)
        first_byte = data[0]
        raise MagicByteError.new(first_byte) if first_byte != MAGIC_BYTE
      end

      def build_model_map(models)
        models.each_with_object(Hash.new) do |model, map|
          key = model_key(model)
          raise DuplicateKeyError.new(map[key], model) if map.key?(key) && !model.equal?(map[key])
          map[key] = model
        end
      end
    end
  end
end

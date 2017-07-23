module Avromatic
  module Model

    # This concern adds support for serialization based on AvroTurf::Messaging.
    # This serialization leverages a schema registry to prefix encoded values
    # with an id for the schema.
    module MessagingSerialization
      extend ActiveSupport::Concern

      delegate :avro_messaging, to: :class
      private :avro_messaging

      module Encode
        def avro_message_value
          @avro_message_value ||= avro_messaging.encode(
            value_attributes_for_avro,
            schema_name: value_avro_schema.fullname
          )
        end

        def avro_message_key
          raise 'Model has no key schema' unless key_avro_schema
          @avro_message_key ||= avro_messaging.encode(
            key_attributes_for_avro,
            schema_name: key_avro_schema.fullname
          )
        end
      end
      include Encode

      # This module provides methods to decode an Avro-encoded value and
      # an optional Avro-encoded key as a new model instance.
      module Decode

        # If two arguments are specified then the first is interpreted as the
        # message key and the second is the message value. If there is only one
        # arg then it is used as the message value.
        def avro_message_decode(*args)
          new(avro_message_attributes(*args))
        end

        def avro_message_attributes(*args)
          message_key, message_value = args.size > 1 ? args : [nil, args.first]
          key_attributes = message_key &&
            avro_messaging.decode(message_key, schema_name: key_avro_schema.fullname)
          value_attributes = avro_messaging
            .decode(message_value, schema_name: avro_schema.fullname)

          value_attributes.merge!(key_attributes || {})
        end
      end

      module Registration
        def register_schemas!
          register_schema(key_avro_schema) if key_avro_schema
          register_schema(value_avro_schema)
          nil
        end

        private

        def register_schema(schema)
          avro_messaging.registry.register(schema.fullname, schema)
        end
      end

      module ClassMethods
        # The messaging object acts as an intermediary talking to the schema
        # registry and using returned/specified schemas to decode/encode.
        def avro_messaging
          Avromatic.messaging
        end

        include Decode
        include Registration
      end
    end
  end
end

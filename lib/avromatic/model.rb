# frozen_string_literal: true

require 'avromatic/model/builder'
require 'avromatic/model/coercion_error'
require 'avromatic/model/message_decoder'
require 'avromatic/model/custom_type_registry'
require 'avromatic/model/validation_error'

module Avromatic
  module Model

    # Returns a module that can be included in a class to define a model
    # based on Avro schema(s).
    #
    # Example:
    #   class MyTopic
    #     include Avromatic::Model.build(schema_name: :topic_value,
    #                                      key_schema_name: :topic_key)
    #   end
    #
    # Either schema(_name) or value_schema(_name) must be specified.
    #
    # value_schema(_name) is handled identically to schema(_name) and is
    # treated like an alias for use when both a value and a key schema are
    # specified.
    #
    # Options:
    #   value_schema_name:
    #     The full name of an Avro schema. The schema will be loaded
    #     using the schema store.
    #   value_schema:
    #     An Avro::Schema.
    #   schema_name:
    #     The full name of an Avro schema. The schema will be loaded
    #     using the schema store.
    #   schema:
    #     An Avro::Schema.
    #   key_schema_name:
    #     The full name of an Avro schema for the key. When an instance of
    #     the model is encoded, this schema will be used to encode the key.
    #     The schema will be loaded using the schema store.
    #   key_schema:
    #     An Avro::Schema for the key.
    def self.build(**options)
      Builder.new(**options).mod
    end

    # Returns an anonymous class, that can be assigned to a constant,
    # defined based on Avro schema(s). See Avromatic::Model.build.
    def self.model(**options)
      Builder.model(**options)
    end
  end
end

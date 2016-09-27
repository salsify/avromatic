require 'virtus'
require 'active_support/concern'
require 'active_model'
require 'avromatic/model/configuration'
require 'avromatic/model/value_object'
require 'avromatic/model/configurable'
require 'avromatic/model/nested_models'
require 'avromatic/model/validation'
require 'avromatic/model/attribute/union'
require 'avromatic/model/attributes'
require 'avromatic/model/attribute/record'
require 'avromatic/model/raw_serialization'
require 'avromatic/model/messaging_serialization'

module Avromatic
  module Model

    # This class implements generating models from Avro schemas.
    class Builder

      attr_reader :mod, :config

      # For options see Avromatic::Model.build
      def self.model(**options)
        Class.new do
          include Avromatic::Model::Builder.new(**options).mod

          # Name is required for attribute validations on an anonymous class.
          def self.name
            super || (@name ||= config.avro_schema.name.classify)
          end
        end
      end

      # For options see Avromatic::Model.build
      def initialize(**options)
        @mod = Module.new
        @config = Avromatic::Model::Configuration.new(**options)
        define_included_method
      end

      def inclusions
        [
          ActiveModel::Validations,
          Virtus.value_object,
          Avromatic::Model::Configurable,
          Avromatic::Model::NestedModels,
          Avromatic::Model::Validation,
          Avromatic::Model::Attributes,
          Avromatic::Model::ValueObject,
          Avromatic::Model::RawSerialization,
          Avromatic::Model::MessagingSerialization
        ]
      end

      private

      def define_included_method
        with_builder do |builder|
          mod.define_singleton_method(:included) do |model_class|
            model_class.include(*builder.inclusions)
            model_class.config = builder.config
            model_class.add_avro_fields
          end
        end
      end

      def with_builder
        yield(self)
      end
    end
  end
end

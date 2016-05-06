require 'virtus'
require 'active_support/concern'
require 'active_model'
require 'salsify_avro/model/configuration'
require 'salsify_avro/model/value_object'
require 'salsify_avro/model/configurable'
require 'salsify_avro/model/attributes'
require 'salsify_avro/model/serialization'

module SalsifyAvro
  module Model

    # This class implements generating models from Avro schemas.
    class Builder

      attr_reader :mod, :config

      # For options see SalsifyAvro::Model.build
      def self.model(**options)
        Class.new do
          include SalsifyAvro::Model::Builder.new(**options).mod

          # Name is required for attribute validations on an anonymous class.
          def self.name
            super || (@name ||= config.avro_schema.name.classify)
          end
        end
      end

      # For options see SalsifyAvro::Model.build
      def initialize(**options)
        @mod = Module.new
        @config = SalsifyAvro::Model::Configuration.new(**options)
        define_included_method
      end

      def inclusions
        [
          ActiveModel::Validations,
          Virtus.value_object,
          SalsifyAvro::Model::Configurable,
          SalsifyAvro::Model::Attributes,
          SalsifyAvro::Model::ValueObject,
          SalsifyAvro::Model::Serialization
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

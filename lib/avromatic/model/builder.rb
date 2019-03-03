# frozen_string_literal: true

require 'active_support/concern'
require 'active_model'
require 'avromatic/model/configuration'
require 'avromatic/model/value_object'
require 'avromatic/model/configurable'
require 'avromatic/model/field_helper'
require 'avromatic/model/nested_models'
require 'avromatic/model/validation'
require 'avromatic/model/types/type_factory'
require 'avromatic/model/attributes'
require 'avromatic/model/raw_serialization'
require 'avromatic/model/messaging_serialization'

module Avromatic
  module Model

    # This class implements generating models from Avro schemas.
    class Builder

      attr_reader :mod, :config

      # For options see Avromatic::Model.build
      def self.model(**options, &block)
        Class.new do
          include Avromatic::Model::Builder.new(**options).mod

          # Name is required for attribute validations on an anonymous class.
          def self.name
            super || (@name ||= config.avro_schema.name.classify)
          end

          class_eval(&block) if block
        end
      end

      # For options see Avromatic::Model.build
      def initialize(**options)
        if options[:native] == false
          options = options.reverse_merge(
            nested_models: ModelRegistry.new
          )
          @config = Avromatic::Model::Configuration.new(**options)
          @mod = Module.new
          define_included_method
        else
          @config = Avromatic::Model::Configuration.new(**options)
          define_native_module
        end
      end

      def build
        AvromaticModel.build(schema: config.avro_schema.to_json)
      end

      def inclusions
        [
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

      def define_native_module
        @mod = AvromaticModel.build(config)
      end

      def define_included_method
        local_mod = mod
        local_builder = self
        mod.define_singleton_method(:included) do |model_class|
          model_class.include(*local_builder.inclusions)
          model_class.config = local_builder.config
          model_class.add_avro_fields(local_mod)
        end
      end
    end
  end
end

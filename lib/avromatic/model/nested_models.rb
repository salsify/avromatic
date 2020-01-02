# frozen_string_literal: true

require 'active_support/inflector/methods'

module Avromatic
  module Model
    # This module handles integration with the ModelRegistry and support
    # for nested model reuse.
    module NestedModels
      extend ActiveSupport::Concern

      module ClassMethods
        # Register this model if it can be used as a nested model.
        def register!
          return unless key_avro_schema.nil? && value_avro_schema.type_sym == :record

          roots = [self]
          until roots.empty?
            model = roots.shift
            next if nested_models.registered?(model)

            nested_models.register(model)
            roots.concat(model.referenced_model_classes)
          end
        end

        def referenced_model_classes
          attribute_definitions.values.flat_map { |definition| definition.type.referenced_model_classes }.freeze
        end
      end
    end
  end
end

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

          processed = Set.new
          roots = [self]
          until roots.empty?
            model = roots.shift
            # Avoid any nested model dependency cycles by ignoring already processed models
            next unless processed.add?(model)

            nested_models.ensure_registered_model(model)
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

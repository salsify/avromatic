require 'active_support/inflector/methods'

module Avromatic
  module Model
    # This module handles integration with the ModelRegistry and support
    # for nested model reuse.
    module NestedModels
      extend ActiveSupport::Concern

      module ClassMethods
        def build_nested_model(schema)
          fullname = nested_models.remove_prefix(schema.fullname)

          if nested_models.registered?(fullname)
            nested_models[fullname]
          else
            Avromatic::Model.model(schema: schema,
                                   nested_models: nested_models)
          end
        end

        # Register this model if it can be used as a nested model.
        def register!
          if key_avro_schema.nil? && value_avro_schema.type_sym == :record
            nested_models.register(self)
          end
        end
      end
    end
  end
end

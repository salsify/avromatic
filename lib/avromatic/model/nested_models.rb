require 'active_support/inflector/methods'

module Avromatic
  module Model
    # This module handles integration with the ModelRegistry and support
    # for nested model reuse.
    module NestedModels
      extend ActiveSupport::Concern

      module ClassMethods
        def build_nested_model(schema)
          fullname = schema.fullname

          if nested_models.model?(fullname)
            nested_models[fullname]
          else
            nested_model = Avromatic::Model.model(schema: schema,
                                                  nested_models: nested_models)
            Axiom::Types::Object.new { primitive(nested_model) }
            nested_models.register(nested_model)
          end
        end
      end
    end
  end
end

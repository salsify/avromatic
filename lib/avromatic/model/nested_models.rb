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
          if key_avro_schema.nil? && value_avro_schema.type_sym == :record
            nested_models.register(self)
          end
        end
      end
    end
  end
end

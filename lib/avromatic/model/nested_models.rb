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
            # Register the generated model with Axiom to prevent it being
            # treated as a BasicObject.
            # See https://github.com/solnic/virtus/issues/284#issuecomment-56405137
            nested_model = self
            Axiom::Types::Object.new { primitive(nested_model) }

            nested_models.register(self)
          end
        end

        def method_missing(name, *_args)
          fullname = registered_model_fullname(name)
          fullname ? define_nested_model_method(name, fullname) : super
        end

        def respond_to_missing?(name, _include_all)
          !!registered_model_fullname(name) || super
        end

        private

        def define_nested_model_method(method_name, fullname)
          nested_models[fullname].tap do |nested_model|
            define_singleton_method(method_name) { nested_model }
          end
        end

        def registered_model_fullname(name)
          fullname = fullname_from_method(name)
          fullname if fullname && nested_models.registered?(fullname)
        end

        def fullname_from_method(name)
          name.to_s.gsub('__', '.').sub!(/_model$/, '')
        end
      end
    end
  end
end

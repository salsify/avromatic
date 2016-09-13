require 'active_support/inflector/methods'

module Avromatic
  module Model
    module Models
      extend ActiveSupport::Concern

      module ClassMethods
        # Hash of generated models by field name
        def models
          @models ||= {}
        end

        def add_model(field_name, model)
          models[field_name.to_sym] = model
        end

        def build_model(schema)
          name_parts = get_name_parts(schema)
          model_name = name_parts.pop

          parent_module = define_modules(name_parts)

          module_fetch(parent_module, model_name) do
            Avromatic::Model.model(schema: schema,
                                   module: self.module)
          end.tap do |model|
            # Register the generated model with Axiom to prevent it being
            # treated as a BasicObject.
            # See https://github.com/solnic/virtus/issues/284#issuecomment-56405137
            Axiom::Types::Object.new { primitive(model) }
          end
        end

        private

        def define_modules(name_parts)
          parent_module = self.module
          name_parts.each do |part|
            parent_module = module_fetch(parent_module, part) { Module.new }
          end
          parent_module
        end

        def module_fetch(mod, name)
          if mod.const_defined?(name, false)
            mod.const_get(name)
          else
            mod.const_set(name, yield)
          end
        end

        def get_name_parts(schema)
          name = schema.fullname
          prefix = strip_namespace_prefix
          if prefix
            if prefix.is_a?(String)
              name = name[prefix.length..-1] if name.starts_with?(prefix)
            elsif prefix.is_a?(Regexp)
              name = name.sub(prefix, '')
            else
              raise "unsupported strip_namespace_prefix value: #{prefix}"
            end
          end

          name.split('.').reject(&:blank?).map(&:camelize)
        end
      end
    end
  end
end

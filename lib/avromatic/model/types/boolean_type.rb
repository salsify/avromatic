module Avromatic
  module Model
    module Types

      # This subclass of Virtus::Attribute is defined to ensure that Avromatic
      # generated models (identified by their inclusion of
      # Avromatic::Model::Attributes) are always coerced by identifying an
      # instance of the model or creating a new one.
      # This is required to coerce models correctly with nested complex types
      # with Virtus.
      class BooleanType
        VALUE_CLASSES = [::TrueClass, ::FalseClass].freeze

        def value_classes
          VALUE_CLASSES
        end

        def coerce(input)
          if input.nil? || input.is_a?(::TrueClass) || input.is_a?(::FalseClass)
            input
          else
            raise ArgumentError.new("Could not coerce '#{input.inspect}' to a Boolean")
          end
        end

        def coercible?(input)
          input.nil? || input.is_a?(::TrueClass) || input.is_a?(::FalseClass)
        end

        alias_method :coerced?, :coercible?

        # TODO: Unused
        def serialize(value)
          value
        end
      end
    end
  end
end

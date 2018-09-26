module Avromatic
  module Model
    module Types
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

        def serialize(value, **)
          value
        end
      end
    end
  end
end

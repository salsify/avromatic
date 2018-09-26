module Avromatic
  module Model
    module Types
      class FloatType
        VALUE_CLASSES = [::Float].freeze

        def value_classes
          VALUE_CLASSES
        end

        def coerce(input)
          if input.nil? || input.is_a?(::Float)
            input
          else
            raise ArgumentError.new("Could not coerce '#{input.inspect}' to a Float")
          end
        end

        def coercible?(input)
          input.nil? || input.is_a?(::Float)
        end

        alias_method :coerced?, :coercible?

        def serialize(value, **)
          value
        end
      end
    end
  end
end

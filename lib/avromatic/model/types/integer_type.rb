module Avromatic
  module Model
    module Types
      class IntegerType
        VALUE_CLASSES = [::Integer].freeze

        def value_classes
          VALUE_CLASSES
        end

        def coerce(input)
          if input.nil? || input.is_a?(::Integer)
            input
          else
            raise Avromatic::Model::CoercionError.new("Could not coerce '#{input.inspect}' to an Integer")
          end
        end

        def coercible?(input)
          input.nil? || input.is_a?(::Integer)
        end

        alias_method :coerced?, :coercible?

        def serialize(value, **)
          value
        end
      end
    end
  end
end

module Avromatic
  module Model
    module Types
      class FixedType
        VALUE_CLASSES = [::String].freeze

        attr_reader :size

        def initialize(size)
          @size = size
        end

        def value_classes
          VALUE_CLASSES
        end

        def coerce(input)
          if coercible?(input)
            input
          else
            raise Avromatic::Model::CoercionError.new("Could not coerce '#{input.inspect}' to a Fixed(#{size})")
          end
        end

        def coercible?(input)
          input.nil? || (input.is_a?(::String) && input.length == size)
        end

        alias_method :coerced?, :coercible?

        def serialize(value, **)
          value
        end
      end
    end
  end
end

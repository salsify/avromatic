module Avromatic
  module Model
    module Types
      class NullType
        VALUE_CLASSES = [::NilClass].freeze

        def value_classes
          VALUE_CLASSES
        end

        def coerce(input)
          if input.nil?
            nil
          else
            raise Avromatic::Model::CoercionError.new("Could not coerce '#{input.inspect}' to a Null")
          end
        end

        def coercible?(input)
          input.nil?
        end

        alias_method :coerced?, :coercible?

        def serialize(_value, **)
          nil
        end
      end
    end
  end
end

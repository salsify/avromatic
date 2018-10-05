module Avromatic
  module Model
    module Types
      class EnumType
        VALUE_CLASSES = [::String].freeze

        attr_reader :allowed_values

        def initialize(allowed_values)
          @allowed_values = allowed_values.to_set
        end

        def value_classes
          VALUE_CLASSES
        end

        def coerce(input)
          if input.nil?
            input
          elsif coercible?(input)
            input.to_s
          else
            raise Avromatic::Model::CoercionError.new("Could not coerce '#{input.inspect}' to an Enum(#{allowed_values.to_a.join(',')})")
          end
        end

        def coerced?(input)
          input.nil? || (input.is_a?(::String) && allowed_values.include?(input))
        end

        def coercible?(input)
          input.nil? ||
            (input.is_a?(::String) && allowed_values.include?(input)) ||
            (input.is_a?(::Symbol) && allowed_values.include?(input.to_s))
        end

        def serialize(value, **)
          value
        end
      end
    end
  end
end

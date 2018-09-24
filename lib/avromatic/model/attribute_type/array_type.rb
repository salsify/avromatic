module Avromatic
  module Model
    module AttributeType

      # This subclass of Virtus::Attribute is defined to ensure that Avromatic
      # generated models (identified by their inclusion of
      # Avromatic::Model::Attributes) are always coerced by identifying an
      # instance of the model or creating a new one.
      # This is required to coerce models correctly with nested complex types
      # with Virtus.
      class ArrayType
        attr_reader :value_type

        def initialize(value_type:)
          @value_type = value_type
        end

        def value_classes
          [::Array]
        end

        def coerce(input)
          if input.nil?
            input
          elsif input.is_a?(::Array)
            input.map { |element_input| value_type.coerce(element_input) }
          else
            raise ArgumentError.new("Could not coerce '#{input.inspect}' to an Array")
          end
        end

        def coercible?(input)
          input.nil? || (input.is_a?(::Array) && input.all? { |element_input| value_type.coercible?(element_input) })
        end

        def coerced?(value)
          value.nil? || (value.is_a?(::Array) && value.all? { |element| value_type.coerced?(element) })
        end

        # TODO: Unused
        def serialize(value)
          if value.nil?
            value
          else
            value.map { |element| value_type.serialize(element) }
          end
        end
      end
    end
  end
end

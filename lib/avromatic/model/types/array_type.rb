# frozen_string_literal: true

module Avromatic
  module Model
    module Types
      class ArrayType
        VALUE_CLASSES = [::Array].freeze
        attr_reader :value_type

        def initialize(value_type:)
          @value_type = value_type
        end

        def value_classes
          VALUE_CLASSES
        end

        def name
          "array[#{value_type.name}]"
        end

        def coerce(input)
          if input.nil?
            input
          elsif input.is_a?(::Array)
            input.map { |element_input| value_type.coerce(element_input) }
          else
            raise ArgumentError.new("Could not coerce '#{input.inspect}' to #{name}")
          end
        end

        def coercible?(input)
          input.nil? || (input.is_a?(::Array) && input.all? { |element_input| value_type.coercible?(element_input) })
        end

        def coerced?(value)
          value.nil? || (value.is_a?(::Array) && value.all? { |element| value_type.coerced?(element) })
        end

        def serialize(value, strict:)
          if value.nil?
            value
          else
            value.map { |element| value_type.serialize(element, strict: strict) }
          end
        end
      end
    end
  end
end

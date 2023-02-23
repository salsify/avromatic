# frozen_string_literal: true

require 'avromatic/model/types/abstract_type'

module Avromatic
  module Model
    module Types
      class ArrayType < AbstractType
        VALUE_CLASSES = [::Array].freeze

        attr_reader :value_type

        def initialize(value_type:)
          super()
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
          value.nil? || (value.is_a?(::Array) && value.all? { |element_input| value_type.coerced?(element_input) })
        end

        # we can take any coercible nested values for union types for backward-compatibility
        alias_method :matched?, :coercible?

        def serialize(value, strict)
          if value.nil?
            value
          else
            value.map { |element| value_type.serialize(element, strict) }
          end
        end

        def referenced_model_classes
          value_type.referenced_model_classes
        end
      end
    end
  end
end

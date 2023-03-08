# frozen_string_literal: true

require 'bigdecimal'
require 'bigdecimal/util'
require 'avromatic/model/types/abstract_type'

module Avromatic
  module Model
    module Types
      class DecimalType < AbstractType
        VALUE_CLASSES = [::BigDecimal].freeze
        INPUT_CLASSES = [::BigDecimal, ::Float, ::Integer].freeze

        attr_reader :precision, :scale

        def initialize(precision:, scale: 0)
          super()
          @precision = precision
          @scale = scale
        end

        def value_classes
          VALUE_CLASSES
        end

        def input_classes
          INPUT_CLASSES
        end

        def name
          "decimal(#{precision}, #{scale})"
        end

        def coerce(input)
          case input
          when ::NilClass, ::BigDecimal
            input
          when ::Float, ::Integer
            input.to_d
          else
            raise ArgumentError.new("Could not coerce '#{input.inspect}' to #{name}")
          end
        end

        def coercible?(input)
          input.nil? || input_classes.any? { |input_class| input.is_a?(input_class) }
        end

        def coerced?(value)
          value.nil? || value_classes.any? { |value_class| value.is_a?(value_class) }
        end

        def serialize(value, _strict)
          value
        end

        def referenced_model_classes
          EMPTY_ARRAY
        end
      end
    end
  end
end

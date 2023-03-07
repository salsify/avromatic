# frozen_string_literal: true

require 'bigdecimal'
require 'bigdecimal/util'
require 'avromatic/model/types/abstract_type'

module Avromatic
  module Model
    module Types
      class DecimalType < AbstractType
        VALUE_CLASSES = [::Numeric].freeze
        INPUT_CLASSES = [::Numeric, ::String].freeze

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
          when ::Rational
            input.to_d(precision)
          when ::Numeric, ::String
            input.to_d
          else
            raise ArgumentError.new("Could not coerce '#{input.inspect}' to #{name}")
          end
        end

        def coercible?(input)
          input.nil? || input.is_a?(::String) || input.is_a?(::Numeric)
        end

        def coerced?(input)
          input.nil? || input.is_a?(::BigDecimal)
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

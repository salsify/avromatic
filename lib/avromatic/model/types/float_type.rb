# frozen_string_literal: true

require 'avromatic/model/types/abstract_type'

module Avromatic
  module Model
    module Types
      class FloatType < AbstractType
        VALUE_CLASSES = [::Float].freeze
        INPUT_CLASSES = [::Float, ::Integer].freeze

        def value_classes
          VALUE_CLASSES
        end

        def input_classes
          INPUT_CLASSES
        end

        def name
          'float'
        end

        def coerce(input)
          if input.nil? || input.is_a?(::Float)
            input
          elsif input.is_a?(::Integer)
            input.to_f
          else
            raise ArgumentError.new("Could not coerce '#{input.inspect}' to #{name}")
          end
        end

        def coercible?(input)
          input.nil? || input.is_a?(::Float) || input.is_a?(::Integer)
        end

        def coerced?(input)
          input.nil? || input.is_a?(::Float)
        end

        def serialize(value, **)
          value
        end

        def referenced_model_classes
          EMPTY_ARRAY
        end
      end
    end
  end
end

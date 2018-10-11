# frozen_string_literal: true

module Avromatic
  module Model
    module Types
      class FloatType
        VALUE_CLASSES = [::Float].freeze

        def value_classes
          VALUE_CLASSES
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
      end
    end
  end
end

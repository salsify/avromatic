# frozen_string_literal: true

module Avromatic
  module Model
    module Types
      class StringType
        VALUE_CLASSES = [::String].freeze

        def value_classes
          VALUE_CLASSES
        end

        def name
          'string'
        end

        def coerce(input)
          if input.nil? || input.is_a?(::String)
            input
          elsif input.is_a?(::Symbol)
            input.to_s
          else
            raise ArgumentError.new("Could not coerce '#{input.inspect}' to #{name}")
          end
        end

        def coercible?(input)
          input.nil? || input.is_a?(::String) || input.is_a?(::Symbol)
        end

        def coerced?(value)
          value.nil? || value.is_a?(::String)
        end

        def serialize(value, **)
          value
        end
      end
    end
  end
end

# frozen_string_literal: true

module Avromatic
  module Model
    module Types
      class FixedType
        VALUE_CLASSES = [::String].freeze

        attr_reader :size

        def initialize(size)
          @size = size
        end

        def name
          "fixed(#{size})"
        end

        def value_classes
          VALUE_CLASSES
        end

        def coerce(input)
          if coercible?(input)
            input
          else
            raise ArgumentError.new("Could not coerce '#{input.inspect}' to #{name}")
          end
        end

        def coercible?(input)
          input.nil? || (input.is_a?(::String) && input.length == size)
        end

        alias_method :coerced?, :coercible?

        def serialize(value, **)
          value
        end
      end
    end
  end
end

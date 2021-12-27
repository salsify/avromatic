# frozen_string_literal: true

require 'avromatic/model/types/abstract_type'

module Avromatic
  module Model
    module Types
      class FixedType < AbstractType
        VALUE_CLASSES = [::String].freeze

        attr_reader :size

        def initialize(size)
          super()
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

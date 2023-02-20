# frozen_string_literal: true

require 'avromatic/model/types/abstract_type'

module Avromatic
  module Model
    module Types
      class BigIntType < AbstractType
        VALUE_CLASSES = [::Integer].freeze

        MAX_RANGE = 2 ** 63

        def value_classes
          VALUE_CLASSES
        end

        def name
          'bigint'
        end

        def coerce(input)
          if coercible?(input)
            input
          else
            raise ArgumentError.new("Could not coerce '#{input.inspect}' to #{name}")
          end
        end

        def coercible?(input)
          input.nil? || (input.is_a?(::Integer) && input.between?(-MAX_RANGE, MAX_RANGE - 1))
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

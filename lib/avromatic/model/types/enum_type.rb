# frozen_string_literal: true

require 'avromatic/model/types/abstract_type'

module Avromatic
  module Model
    module Types
      class EnumType < AbstractType
        VALUE_CLASSES = [::String].freeze
        INPUT_CLASSES = [::String, ::Symbol].freeze

        attr_reader :allowed_values

        def initialize(allowed_values)
          super()
          @allowed_values = allowed_values.to_set
        end

        def name
          "enum#{allowed_values.to_a}"
        end

        def value_classes
          VALUE_CLASSES
        end

        def input_classes
          INPUT_CLASSES
        end

        def coerce(input)
          if input.nil?
            input
          elsif coercible?(input)
            input.to_s
          else
            raise ArgumentError.new("Could not coerce '#{input.inspect}' to #{name}")
          end
        end

        def coerced?(value)
          value.nil? || value.is_a?(::String) && allowed_values.include?(value)
        end

        def coercible?(input)
          input.nil? ||
            (input.is_a?(::String) && allowed_values.include?(input)) ||
            (input.is_a?(::Symbol) && allowed_values.include?(input.to_s))
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

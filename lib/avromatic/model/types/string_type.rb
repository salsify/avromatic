# frozen_string_literal: true

require 'avromatic/model/types/abstract_type'

module Avromatic
  module Model
    module Types
      class StringType < AbstractType
        VALUE_CLASSES = [::String].freeze
        INPUT_CLASSES = [::String, ::Symbol].freeze

        def value_classes
          VALUE_CLASSES
        end

        def input_classes
          INPUT_CLASSES
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

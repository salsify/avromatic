# frozen_string_literal: true

require 'avromatic/model/types/abstract_type'

module Avromatic
  module Model
    module Types
      class DateType < AbstractType
        VALUE_CLASSES = [::Date].freeze
        INPUT_CLASSES = [::Date, ::Time].freeze

        def value_classes
          VALUE_CLASSES
        end

        def input_classes
          INPUT_CLASSES
        end

        def name
          'date'
        end

        def coerce(input)
          if input.is_a?(::Time) || input.is_a?(::DateTime)
            ::Date.new(input.year, input.month, input.day)
          elsif input.nil? || input.is_a?(::Date)
            input
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

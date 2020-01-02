# frozen_string_literal: true

require 'avromatic/model/types/abstract_type'

module Avromatic
  module Model
    module Types
      class MapType < AbstractType
        VALUE_CLASSES = [::Hash].freeze

        attr_reader :value_type, :key_type

        def initialize(key_type:, value_type:)
          @key_type = key_type
          @value_type = value_type
        end

        def name
          "map[#{key_type.name} => #{value_type.name}]"
        end

        def value_classes
          VALUE_CLASSES
        end

        def coerce(input)
          if input.nil?
            input
          elsif input.is_a?(::Hash)
            input.each_with_object({}) do |(key_input, value_input), result|
              result[key_type.coerce(key_input)] = value_type.coerce(value_input)
            end
          else
            raise ArgumentError.new("Could not coerce '#{input.inspect}' to #{name}")
          end
        end

        def coercible?(input)
          if input.nil?
            true
          elsif input.is_a?(Hash)
            input.all? do |key_input, value_input|
              key_type.coercible?(key_input) && value_type.coercible?(value_input)
            end
          else
            false
          end
        end

        def coerced?(value)
          if value.nil?
            true
          elsif value.is_a?(Hash)
            value.all? do |element_key, element_value|
              key_type.coerced?(element_key) && value_type.coerced?(element_value)
            end
          else
            false
          end
        end

        def serialize(value, strict:)
          if value.nil?
            value
          else
            value.each_with_object({}) do |(element_key, element_value), result|
              result[key_type.serialize(element_key, strict: strict)] = value_type.serialize(element_value, strict: strict)
            end
          end
        end

        def referenced_model_classes
          [key_type, value_type].tap(&:uniq!).flat_map(&:referenced_model_classes).tap(&:uniq!)
        end
      end
    end
  end
end

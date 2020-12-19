# frozen_string_literal: true

require 'avromatic/model/types/abstract_type'

module Avromatic
  module Model
    module Types
      class ArrayType < AbstractType
        VALUE_CLASSES = [::Array].freeze

        attr_reader :value_type

        def initialize(value_type:)
          @value_type = value_type
        end

        def value_classes
          VALUE_CLASSES
        end

        def name
          "array[#{value_type.name}]"
        end

        def coerce(input)
          if input.nil?
            input
          elsif input.is_a?(::Array)
            input.map { |element_input| value_type.coerce(element_input) }
          else
            raise ArgumentError.new("Could not coerce '#{input.inspect}' to #{name}")
          end
        end

        def coercible?(input)
          input.nil? || (input.is_a?(::Array) && input.all? { |element_input| value_type.coercible?(element_input) })
        end

        def coerced?(value)
          value.nil? || (value.is_a?(::Array) && value.all? { |element| value_type.coerced?(element) })
        end

        def serialize(value, strict)
          if value.nil?
            value
          else
            missing_attributes = nil
            avro_hash = value.each_with_index.with_object([]) do |(element, index), result|
              begin
                result << value_type.serialize(element, strict)
              rescue Avromatic::Model::ValidationError => e
                missing_attributes ||= []
                e.missing_attributes.each do |nested_attribute|
                  missing_attributes << nested_attribute.prepend_array_access(index)
                end
              end
            end

            if missing_attributes.present?
              message = 'Array cannot be serialized because the following attributes are nil: ' \
                  "#{missing_attributes.map(&:to_s).join(', ')}"
              raise Avromatic::Model::ValidationError.new(message, missing_attributes)
            else
              avro_hash
            end
          end
        end

        def referenced_model_classes
          value_type.referenced_model_classes
        end
      end
    end
  end
end

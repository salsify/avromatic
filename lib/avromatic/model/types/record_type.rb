# frozen_string_literal: true

require 'avromatic/model/types/abstract_type'

module Avromatic
  module Model
    module Types
      class RecordType < AbstractType
        attr_reader :record_class, :value_classes, :input_classes

        def initialize(record_class:)
          super()
          @record_class = record_class
          @value_classes = [record_class].freeze
          @input_classes = [record_class, Hash].freeze
        end

        def name
          record_class.name.to_s.freeze
        end

        def coerce(input)
          if input.nil? || input.is_a?(record_class)
            input
          elsif input.is_a?(Hash)
            record_class.new(input)
          else
            raise ArgumentError.new("Could not coerce '#{input.inspect}' to #{name}")
          end
        end

        def coercible?(input)
          # TODO: Is there a better way to figure this out?
          input.nil? || input.is_a?(record_class) || coerce(input).valid?
        rescue StandardError
          false
        end

        def serialize(value, strict)
          if value.nil?
            value
          elsif strict
            value.avro_value_datum
          else
            value.value_attributes_for_avro
          end
        end

        def referenced_model_classes
          [record_class].freeze
        end
      end
    end
  end
end

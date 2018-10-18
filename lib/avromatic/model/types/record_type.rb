# frozen_string_literal: true

require 'avromatic/model/types/abstract_type'

module Avromatic
  module Model
    module Types
      class RecordType < AbstractType
        attr_reader :record_class, :value_classes, :input_classes

        def initialize(record_class:)
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

        def coerced?(value)
          value.nil? || value.is_a?(record_class)
        end

        def serialize(value, strict:)
          if value.nil?
            value
          elsif !strict && Avromatic.use_custom_datum_writer && Avromatic.use_encoding_providers? && !record_class.config.mutable
            # n.b. Ideally we'd just return value here instead of wrapping it in a
            # hash but then we'd have no place to stash the union member index...
            { Avromatic::IO::ENCODING_PROVIDER => value }
          else
            # This is only used for recursive serialization so validation has already been done
            strict ? value.avro_value_datum(validate: false) : value.value_attributes_for_avro(validate: false)
          end
        end
      end
    end
  end
end

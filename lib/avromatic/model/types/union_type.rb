# frozen_string_literal: true

require 'avromatic/io'
require 'avromatic/model/types/abstract_type'

module Avromatic
  module Model
    module Types
      class UnionType < AbstractType
        attr_reader :member_types, :value_classes, :input_classes

        def initialize(member_types:)
          super()
          @member_types = member_types
          @value_classes = member_types.flat_map(&:value_classes)
          @input_classes = member_types.flat_map(&:input_classes).uniq
        end

        def name
          "union[#{member_types.map(&:name).join(', ')}]"
        end

        def coerce(input)
          return input if coerced?(input)

          result = nil
          if input.is_a?(Avromatic::IO::UnionDatum)
            result = member_types[input.member_index].coerce(input.datum)
          else
            member_types.find do |member_type|
              result = safe_coerce(member_type, input)
            end
          end

          raise ArgumentError.new("Could not coerce '#{input.inspect}' to #{name}") if result.nil?

          result
        end

        def coerced?(value)
          return false if value.is_a?(Avromatic::IO::UnionDatum)

          value.nil? || member_types.any? do |member_type|
            member_type.coerced?(value)
          end
        end

        def coercible?(input)
          return true if value.is_a?(Avromatic::IO::UnionDatum)

          coerced?(input) || member_types.any? do |member_type|
            member_type.coercible?(input)
          end
        end

        def serialize(value, strict)
          # Avromatic does not treat the null of an optional field as part of the union
          return nil if value.nil?

          member_index = find_index(value)
          if member_index.nil?
            raise ArgumentError.new("Expected #{value.inspect} to be one of #{value_classes.map(&:name)}")
          end

          serialized_value = member_types[member_index].serialize(value, strict)
          if !strict && Avromatic.use_custom_datum_writer
            serialized_value = Avromatic::IO::UnionDatum.new(member_index, serialized_value)
          end
          serialized_value
        end

        def referenced_model_classes
          member_types.flat_map(&:referenced_model_classes).tap(&:uniq!).freeze
        end

        private

        def find_index(value)
          member_types.find_index do |member_type|
            member_type.matched?(value)
          end
        end

        def safe_coerce(member_type, input)
          member_type.coerce(input) if member_type.coercible?(input)
        rescue StandardError
          nil
        end
      end
    end
  end
end

# frozen_string_literal: true

require 'avromatic/io'
require 'avromatic/model/types/abstract_type'

module Avromatic
  module Model
    module Types
      class UnionType < AbstractType
        MEMBER_INDEX = ::Avromatic::IO::DatumReader::UNION_MEMBER_INDEX
        attr_reader :member_types, :value_classes, :input_classes

        def initialize(member_types:)
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
          if input.is_a?(Hash) && input.key?(MEMBER_INDEX)
            result = member_types[input.delete(MEMBER_INDEX)].coerce(input)
          else
            member_types.find do |member_type|
              result = safe_coerce(member_type, input)
            end
          end

          unless result
            raise ArgumentError.new("Could not coerce '#{input.inspect}' to #{name}")
          end

          result
        end

        def coerced?(value)
          value.nil? || member_types.any? do |member_type|
            member_type.coerced?(value)
          end
        end

        def coercible?(input)
          coerced?(input) || member_types.any? do |member_type|
            member_type.coercible?(input)
          end
        end

        def serialize(value, strict:)
          # Avromatic does not treat the null of an optional field as part of the union
          return nil if value.nil?

          member_index = find_index(value)
          if member_index.nil?
            raise ArgumentError.new("Expected #{value.inspect} to be one of #{value_classes.map(&:name)}")
          end

          hash = member_types[member_index].serialize(value, strict: strict)
          if !strict && Avromatic.use_custom_datum_writer && value.is_a?(Avromatic::Model::Attributes)
            hash[Avromatic::IO::UNION_MEMBER_INDEX] = member_index
          end
          hash
        end

        def referenced_models
          member_types.flat_map(&:referenced_models).tap(&:uniq!)
        end

        private

        def find_index(value)
          # TODO: Cache this?
          member_types.find_index do |member_type|
            member_type.value_classes.any? { |value_class| value.is_a?(value_class) }
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

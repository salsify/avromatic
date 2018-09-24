require 'avromatic/io'

module Avromatic
  module Model
    module AttributeType

      # This subclass of Virtus::Attribute is used for any unions that are
      # defined as subclasses of the primitive Avromatic::Model::AttributeType::Union.
      # Values are coerced by first checking if they already match one of the
      # member types, and then by attempting to coerce to each member type in
      # order.
      class UnionType
        MEMBER_INDEX = ::Avromatic::IO::DatumReader::UNION_MEMBER_INDEX
        attr_reader :member_types, :value_classes

        def initialize(member_types:)
          @member_types = member_types
          @value_classes = member_types.flat_map(&:value_classes)
        end

        def find_index(value)
          member_types.find_index do |member_type|
            member_type.value_classes.any? { |value_class| value.is_a?(value_class) }
          end
        end

        def coerce(input)
          return input if coerced?(input)

          result = nil
          if input && input.is_a?(Hash) && input.key?(MEMBER_INDEX)
            # This won't work for unions with booleans
            result = member_types[input.delete(MEMBER_INDEX)].coerce(input)
          else
            member_types.find do |member_type|
              result = safe_coerce(member_type, input)
            end
          end
          result
        end

        def coerced?(value)
          member_types.any? do |member_type|
            member_type.coerced?(value)
          end
        end

        def coercible?(input)
          coerced?(input) || member_types.any? do |member_type|
            member_type.coercible?(input)
          end
        end

        private

        def safe_coerce(member_type, input)
          member_type.coerce(input) if member_type.coercible?(value)
        rescue
          nil
        end
      end
    end
  end
end

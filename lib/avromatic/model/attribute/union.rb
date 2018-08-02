require 'avromatic/model/attribute_type/union'
require 'avromatic/io'

module Avromatic
  module Model
    module Attribute

      # This subclass of Virtus::Attribute is used for any unions that are
      # defined as subclasses of the primitive Avromatic::Model::AttributeType::Union.
      # Values are coerced by first checking if they already match one of the
      # member types, and then by attempting to coerce to each member type in
      # order.
      class Union < Virtus::Attribute
        primitive Avromatic::Model::AttributeType::Union

        MEMBER_INDEX = ::Avromatic::IO::DatumReader::UNION_MEMBER_INDEX

        def initialize(*)
          super

          primitive.types.each do |type|
            member_attributes << Virtus::Attribute.build(type)
          end
        end

        def coerce(input)
          return input if value_coerced?(input)

          result = nil
          if input && input.key?(MEMBER_INDEX)
            result = safe_coerce(member_attributes[input.delete(MEMBER_INDEX)], input)
          else
            member_attributes.find do |union_attribute|
              result = safe_coerce(union_attribute, input)
            end
          end
          result
        end

        def value_coerced?(value)
          member_attributes.any? do |union_attribute|
            union_attribute.value_coerced?(value)
          end
        end

        private

        def safe_coerce(member_attribute, input)
          coerced = member_attribute.coerce(input)

          if coerced.is_a?(Avromatic::Model::Attributes)
            coerced if coerced.valid?
          elsif member_attribute.coerced?(coerced)
            coerced
          end
        rescue StandardError
          nil
        end

        def member_attributes
          @member_attributes ||= Array.new
        end
      end
    end
  end
end

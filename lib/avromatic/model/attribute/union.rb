require 'avromatic/model/attribute_type/union'

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

        def initialize(*)
          super

          primitive.types.each do |type|
            member_attributes << Virtus::Attribute.build(type)
          end
        end

        def coerce(input)
          return input if value_coerced?(input)

          result = nil
          member_attributes.find do |union_attribute|
            begin
              coerced = union_attribute.coerce(input)
              result = coerced unless coerced.is_a?(Avromatic::Model::Attributes) && coerced.invalid?
            rescue
              nil
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

        def member_attributes
          @member_attributes ||= Array.new
        end
      end
    end
  end
end

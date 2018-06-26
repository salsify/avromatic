module Avromatic
  module Model
    module Attribute

      # This subclass of Virtus::Attribute is defined to ensure that Avromatic
      # generated models (identified by their inclusion of
      # Avromatic::Model::Attributes) are always coerced by identifying an
      # instance of the model or creating a new one.
      # This is required to coerce models correctly with nested complex types
      # with Virtus.
      class Record < Virtus::Attribute
        primitive Avromatic::Model::Attributes

        def coerce(value)
          return value if value.nil? || value.is_a?(primitive)

          primitive.new(value)
        end

        def value_coerced?(value)
          value.is_a?(primitive)
        end
      end
    end
  end
end

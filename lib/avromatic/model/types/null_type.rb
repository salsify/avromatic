module Avromatic
  module Model
    module Types

      # This subclass of Virtus::Attribute is defined to ensure that Avromatic
      # generated models (identified by their inclusion of
      # Avromatic::Model::Attributes) are always coerced by identifying an
      # instance of the model or creating a new one.
      # This is required to coerce models correctly with nested complex types
      # with Virtus.
      class NullType
        VALUE_CLASSES = [::NilClass].freeze

        def value_classes
          VALUE_CLASSES
        end

        def coerce(input)
          if input.nil?
            nil
          else
            raise ArgumentError.new("Could not coerce '#{input.inspect}' to a Null")
          end
        end

        def coercible?(input)
          input.nil?
        end

        alias_method :coerced?, :coercible?
      end
    end
  end
end

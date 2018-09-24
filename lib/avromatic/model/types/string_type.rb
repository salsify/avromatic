module Avromatic
  module Model
    module Types

      # This subclass of Virtus::Attribute is defined to ensure that Avromatic
      # generated models (identified by their inclusion of
      # Avromatic::Model::Attributes) are always coerced by identifying an
      # instance of the model or creating a new one.
      # This is required to coerce models correctly with nested complex types
      # with Virtus.
      class StringType
        VALUE_CLASSES = [::String].freeze

        def value_classes
          VALUE_CLASSES
        end

        def coerce(input)
          if input.nil? || input.is_a?(::String)
            input
          elsif input.is_a?(::Symbol)
            input.to_s
          else
            raise ArgumentError.new("Could not coerce '#{input.inspect}' to a String")
          end
        end

        def coercible?(input)
          input.nil? || input.is_a?(::String) || input.is_a?(::Symbol)
        end

        def coerced?(value)
          value.nil? || value.is_a?(::String)
        end
      end
    end
  end
end

module Avromatic
  module Model
    module AttributeType

      # This subclass of Virtus::Attribute is defined to ensure that Avromatic
      # generated models (identified by their inclusion of
      # Avromatic::Model::Attributes) are always coerced by identifying an
      # instance of the model or creating a new one.
      # This is required to coerce models correctly with nested complex types
      # with Virtus.
      class MapType
        attr_reader :value_type, :key_type

        def value_classes
          [::Hash]
        end

        def initialize(key_type:, value_type:)
          @key_type = key_type
          @value_type = value_type
        end

        def coerce(input)
          if input.nil?
            input
          elsif input.is_a?(::Hash)
            input.each_with_object({}) do |(key_input, value_input), result|
              result[key_type.coerce(key_input)] = value_type.coerce(value_input)
            end
          else
            raise ArgumentError.new("Could not coerce '#{input.inspect}' to a Map")
          end
        end

        def coercible?(input)
          if input.nil?
            true
          elsif input.is_a?(Hash)
            input.all? do |key_input, value_input|
              key_type.coercible?(key_input) && value_type.coercible?(value_input)
            end
          else
            false
          end
        end

        def coerced?(value)
          if value.nil?
            true
          elsif value.is_a?(Hash)
            value.all? do |key, value|
              key_type.coerced?(key) && value_type.coerced?(value)
            end
          else
            false
          end
        end
      end
    end
  end
end

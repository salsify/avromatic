module Avromatic
  module Model
    module Types
      class CustomType
        IDENTITY_PROC = Proc.new { |value| value }

        attr_reader :custom_type_configuration, :value_classes

        def initialize(custom_type_configuration:, default_value_classes:)
          @custom_type_configuration = custom_type_configuration
          @deserializer = custom_type_configuration.deserializer || IDENTITY_PROC
          @serializer = custom_type_configuration.serializer || IDENTITY_PROC
          @value_classes = if custom_type_configuration.value_class
                             [custom_type_configuration.value_class].freeze
                           else
                             default_value_classes
                           end
        end

        def coerce(input)
          if input.nil?
            input
          else
            @deserializer.call(input)
          end
        end

        def coercible?(input)
          # TODO: Delegate this to optional configuration
          input.nil? || !coerce(input).nil?
        rescue StandardError
          false
        end

        def coerced?(value)
          # TODO: Delegate this to optional configuration
          coerce(value) == value
        rescue StandardError
          false
        end

        def serialize(value, **)
          @serializer.call(value)
        end
      end
    end
  end
end

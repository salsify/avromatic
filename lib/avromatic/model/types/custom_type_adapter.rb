module Avromatic
  module Model
    module Types
      # TODO: Reconcile this with CustomType
      class CustomTypeAdapter
        IDENTITY_PROC = Proc.new { |value| value }

        attr_reader :custom_type, :value_classes

        def initialize(custom_type:, default_value_classes:)
          @custom_type = custom_type
          @deserializer = custom_type.deserializer || IDENTITY_PROC
          @serializer = custom_type.serializer || IDENTITY_PROC
          @value_classes = custom_type.value_class ? [custom_type.value_class].freeze : default_value_classes
        end

        def coerce(input)
          if input.nil?
            input
          else
            @deserializer.call(input)
          end
        end

        def coercible?(input)
          # TODO: Is there a better way to implement this?
          input.nil? || !coerce(input).nil?
        rescue
          false
        end

        def coerced?(value)
          # TODO: Is there a better way to implement this?
          coerce(value) == value
        rescue
          false
        end

        def serialize(value, **)
          @serializer.call(value)
        end
      end
    end
  end
end

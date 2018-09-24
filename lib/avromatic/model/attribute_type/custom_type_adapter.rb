module Avromatic
  module Model
    module AttributeType
      class CustomTypeAdapter
        attr_reader :custom_type

        def initialize(custom_type:)
          @custom_type = custom_type
          @deserializer = custom_type.deserializer || Proc.new { |value| value }
          @serializer = custom_type.serializer || Proc.new { |value| value }
        end

        def value_classes
          [custom_type.value_class]
        end

        def coerce(input)
          if input.nil? || input.is_a?(custom_type.value_class)
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
          value.nil? || value.is_a?(custom_type.value_class)
        end

        # TODO: Unused
        def serialize(value)
          @serializer.call(value)
        end
      end
    end
  end
end

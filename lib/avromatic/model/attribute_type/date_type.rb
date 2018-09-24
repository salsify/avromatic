module Avromatic
  module Model
    module AttributeType
      class DateType
        def value_classes
          [::Date]
        end

        def coerce(input)
          if input.nil? || input.is_a?(::Date)
            input
          else
            # TODO: How are these encoded?
            raise ArgumentError.new("Could not coerce '#{input.inspect}' to a String")
          end
        end

        def coercible?(input)
          input.nil? || input.is_a?(::Date)
        end

        alias_method :coerced?, :coercible?

        # TODO: Unused
        def serialize(value)
          value
        end
      end
    end
  end
end

module Avromatic
  module Model
    module Types
      class DateType
        VALUE_CLASSES = [::Date].freeze

        def value_classes
          VALUE_CLASSES
        end

        def coerce(input)
          if input.nil? || input.is_a?(::Date)
            input
          elsif input.is_a?(::Time)
            Date.new(input.year, input.month, input.day)
          else
            raise ArgumentError.new("Could not coerce '#{input.inspect}' to a Date")
          end
        end

        def coercible?(input)
          input.nil? || input.is_a?(::Date) || input.is_a?(::Time)
        end

        alias_method :coerced?, :coercible?

        def serialize(value, **)
          value
        end
      end
    end
  end
end

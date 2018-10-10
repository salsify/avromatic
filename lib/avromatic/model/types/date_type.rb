module Avromatic
  module Model
    module Types
      class DateType
        VALUE_CLASSES = [::Date].freeze

        def value_classes
          VALUE_CLASSES
        end

        def name
          'date'.freeze
        end

        def coerce(input)
          if input.is_a?(::Time) || input.is_a?(::DateTime)
            ::Date.new(input.year, input.month, input.day)
          elsif input.nil? || input.is_a?(::Date)
            input
          else
            raise ArgumentError.new("Could not coerce '#{input.inspect}' to #{name}")
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

module Avromatic
  module Model
    module Types
      class AbstractTimestampType
        VALUE_CLASSES = [::Time].freeze

        def value_classes
          VALUE_CLASSES
        end

        def coerce(input)
          if input.nil? || coerced?(input)
            input
          elsif input.is_a?(::Time) || input.is_a?(::DateTime)
            coerce_time(input)
          else
            raise Avromatic::Model::CoercionError.new("Could not coerce '#{input.inspect}' to #{self.class.name.demodulize}")
          end
        end

        def coercible?(input)
          input.nil? || input.is_a?(::Time) || input.is_a?(::DateTime)
        end

        def coerced?(_value)
          raise "#{__method__} must be overridden by #{self.class.name}"
        end

        def serialize(value, **)
          value
        end

        private

        def coerce_time(_input)
          raise "#{__method__} must be overridden by #{self.class.name}"
        end
      end
    end
  end
end

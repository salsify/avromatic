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
          elsif input.is_a?(::Time)
            coerce_time(input)
          else
            # TODO: What other coercions do we need to support? Avro encodes these as ints and longs
            raise ArgumentError.new("Could not coerce '#{input.inspect}' to #{self.class.name.demodulize}")
          end
        end

        def coercible?(input)
          input.nil? || input.is_a?(::Time)
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

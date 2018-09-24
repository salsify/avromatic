module Avromatic
  module Model
    module AttributeType

      # This subclass of Virtus::Attribute is used to truncate timestamp values
      # to the supported precision.
      class AbstractTimestampType
        def value_classes
          [::Time]
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

        private

        def coerce_time(_input)
          raise "#{__method__} must be overridden by #{self.class.name}"
        end
      end
    end
  end
end

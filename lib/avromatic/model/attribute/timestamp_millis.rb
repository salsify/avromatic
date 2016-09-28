require 'avromatic/model/attribute/abstract_timestamp'

module Avromatic
  module Model
    module Attribute

      # This subclass is used to truncate timestamp values to milliseconds.
      class TimestampMillis < AbstractTimestamp

        def value_coerced?(value)
          value.is_a?(Time) && value.usec % 1000 == 0
        end

        private

        def coerce_time(value)
          # value is coerced to a local Time
          # The Avro representation of a timestamp is Epoch seconds, independent
          # of time zone.
          Time.at(value.to_i, value.usec / 1000 * 1000)
        end

      end
    end
  end
end

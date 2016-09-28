require 'avromatic/model/attribute/abstract_timestamp'

module Avromatic
  module Model
    module Attribute

      # This subclass is used to truncate timestamp values to microseconds.
      class TimestampMicros < AbstractTimestamp

        def value_coerced?(value)
          value.is_a?(Time) && value.nsec % 1000 == 0
        end

        private

        def coerce_time(value)
          # value is coerced to a local Time
          # The Avro representation of a timestamp is Epoch seconds, independent
          # of time zone.
          Time.at(value.to_i, value.usec)
        end

      end
    end
  end
end

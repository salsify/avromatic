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
          Time.at(value.to_i, value.usec)
        end

      end
    end
  end
end

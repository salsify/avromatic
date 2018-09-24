require 'avromatic/model/attribute_type/abstract_timestamp_type'

module Avromatic
  module Model
    module AttributeType

      # This subclass is used to truncate timestamp values to milliseconds.
      class TimestampMillisType < Avromatic::Model::AttributeType::AbstractTimestampType

        def coerced?(value)
          value.is_a?(::Time) && value.usec % 1000 == 0
        end

        private

        def coerce_time(input)
          # value is coerced to a local Time
          # The Avro representation of a timestamp is Epoch seconds, independent
          # of time zone.
          ::Time.at(input.to_i, input.usec / 1000 * 1000)
        end

      end
    end
  end
end

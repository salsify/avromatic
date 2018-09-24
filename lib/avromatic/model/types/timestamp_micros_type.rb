require 'avromatic/model/types/abstract_timestamp_type'

module Avromatic
  module Model
    module Types

      # This subclass is used to truncate timestamp values to microseconds.
      class TimestampMicrosType < Avromatic::Model::Types::AbstractTimestampType

        def coerced?(value)
          value.is_a?(::Time) && value.nsec % 1000 == 0
        end

        private

        def coerce_time(input)
          # value is coerced to a local Time
          # The Avro representation of a timestamp is Epoch seconds, independent
          # of time zone.
          ::Time.at(input.to_i, input.usec)
        end

      end
    end
  end
end

# frozen_string_literal: true

require 'avromatic/model/types/abstract_timestamp_type'

module Avromatic
  module Model
    module Types

      # This subclass is used to truncate timestamp values to microseconds.
      class TimestampMicrosType < Avromatic::Model::Types::AbstractTimestampType

        def name
          'timestamp-micros'
        end

        private

        def truncated?(value)
          value.nsec % 1000 == 0
        end

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

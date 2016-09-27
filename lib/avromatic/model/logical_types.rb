require 'avromatic/model/attribute/timestamp_micros'
require 'avromatic/model/attribute/timestamp_millis'

module Avromatic
  module Model
    module LogicalTypes

      LOGICAL_TYPE_MAP = {
        'date' => Date,
        'timestamp-micros' => Avromatic::Model::Attribute::TimestampMicros,
        'timestamp-millis' => Avromatic::Model::Attribute::TimestampMillis
      }.freeze

      def self.value_class(logical_type)
        LOGICAL_TYPE_MAP[logical_type]
      end
    end
  end
end

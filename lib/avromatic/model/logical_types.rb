require 'avromatic/model/attribute_type/date_type'
require 'avromatic/model/attribute_type/timestamp_micros_type'
require 'avromatic/model/attribute_type/timestamp_millis_type'

# TODO: Consolidate with other types?
module Avromatic
  module Model
    module LogicalTypes

      LOGICAL_TYPE_MAP = {
        'date' => Avromatic::Model::AttributeType::DateType,
        'timestamp-micros' => Avromatic::Model::AttributeType::TimestampMicrosType,
        'timestamp-millis' => Avromatic::Model::AttributeType::TimestampMillisType
      }.freeze

      def self.value_class(logical_type)
        LOGICAL_TYPE_MAP[logical_type]
      end
    end
  end
end

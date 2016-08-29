module Avromatic
  module Model
    module LogicalTypes

      LOGICAL_TYPE_MAP = {
        'date' => Date,
        'timestamp-micros' => Time,
        'timestamp-millis' => Time
      }.freeze

      def self.value_class(logical_type)
        LOGICAL_TYPE_MAP[logical_type]
      end
    end
  end
end

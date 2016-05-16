module Avromatic
  module Model

    class CustomType

      attr_accessor :to_avro, :from_avro, :value_class

      def initialize(value_class)
        @value_class = value_class
      end
    end
  end
end

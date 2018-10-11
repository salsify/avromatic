# frozen_string_literal: true

module Avromatic
  module Model

    # Instances of this class contains the configuration for custom handling of
    # a named type (record, enum, fixed).
    class CustomTypeConfiguration

      attr_accessor :to_avro, :from_avro, :value_class

      def initialize(value_class)
        @value_class = value_class
      end

      # A deserializer method is used when assigning to the model. It is used both when
      # deserializing a model instance from Avro and when directly instantiating
      # an instance. The deserializer method must accept a single argument and return
      # the value to store in the model for the attribute.
      def deserializer
        proc = from_avro_proc
        wrap_proc(proc) if proc
      end

      # A serializer method is used when preparing attributes to be serialized using
      # Avro. The serializer method must accept a single argument of the model value
      # for the attribute and return a value in a form that Avro can serialize
      # for the attribute.
      def serializer
        proc = to_avro_proc
        wrap_proc(proc) if proc
      end

      private

      def to_avro_proc
        to_avro || value_class_method(:to_avro)
      end

      def from_avro_proc
        from_avro || value_class_method(:from_avro)
      end

      def value_class_method(method_name)
        value_class && value_class.respond_to?(method_name) &&
          value_class.method(method_name).to_proc
      end

      # Wrap the supplied Proc to handle nil.
      def wrap_proc(proc)
        ->(value) { proc.call(value) if value }
      end
    end
  end
end

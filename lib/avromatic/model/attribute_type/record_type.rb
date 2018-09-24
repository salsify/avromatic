module Avromatic
  module Model
    module AttributeType

      # This subclass of Virtus::Attribute is defined to ensure that Avromatic
      # generated models (identified by their inclusion of
      # Avromatic::Model::Attributes) are always coerced by identifying an
      # instance of the model or creating a new one.
      # This is required to coerce models correctly with nested complex types
      # with Virtus.
      class RecordType
        attr_reader :record_class

        def initialize(record_class:)
          @record_class = record_class
        end

        def value_classes
          [record_class]
        end

        def coerce(input)
          # TODO: Should this be lazy to support class reloading?
          if input.nil? || input.is_a?(record_class)
            input
          elsif input.is_a?(Hash)
            record_class.new(input)
          else
            raise ArgumentError.new("Could not coerce '#{input.inspect}' to a #{record_class}")
          end
        end

        def coercible?(input)
          # TODO: Is there a better way to figure this out?
          input.nil? || input.is_a?(record_class) || coerce(input).valid?
        rescue
          false
        end

        def coerced?(value)
          value.nil? || value.is_a?(record_class)
        end
      end
    end
  end
end

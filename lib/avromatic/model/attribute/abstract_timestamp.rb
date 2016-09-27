module Avromatic
  module Model
    module Attribute

      # This subclass of Virtus::Attribute is used to truncate timestamp values
      # to the supported precision.
      class AbstractTimestamp < Virtus::Attribute
        def coerce(value)
          return value if value.nil? || value_coerced?(value)

          value.is_a?(Time) ? coerce_time(value) : value
        end

        def value_coerced?(_value)
          raise "#{__method__} must be overridden by #{self.class.name}"
        end

        private

        def coerce_time(_value)
          raise "#{__method__} must be overridden by #{self.class.name}"
        end
      end
    end
  end
end

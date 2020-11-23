# frozen_string_literal: true

require 'active_support/time_with_zone'

module Avromatic
  module Model
    module Types
      class AbstractTimestampType < AbstractType
        VALUE_CLASSES = [::Time].freeze
        INPUT_CLASSES = [::Time, ::DateTime, ::ActiveSupport::TimeWithZone].freeze

        def value_classes
          VALUE_CLASSES
        end

        def input_classes
          INPUT_CLASSES
        end

        def coerce(input)
          if input.nil? || coerced?(input)
            input
          elsif input.is_a?(::Time) || input.is_a?(::DateTime)
            coerce_time(input)
          else
            raise ArgumentError.new("Could not coerce '#{input.inspect}' to #{name}")
          end
        end

        def coercible?(input)
          input.nil? || input.is_a?(::Time) || input.is_a?(::DateTime)
        end

        def coerced?(value)
          # ActiveSupport::TimeWithZone overrides is_a? is to make it look like a Time
          # even though it's not which can lead to unexpected behavior if we don't force
          # a coercion
          value.is_a?(::Time) && value.class != ActiveSupport::TimeWithZone && truncated?(value)
        end

        def serialize(value, _strict)
          value
        end

        private

        def truncated?(_value)
          raise "#{__method__} must be overridden by #{self.class.name}"
        end

        def coerce_time(_input)
          raise "#{__method__} must be overridden by #{self.class.name}"
        end
      end
    end
  end
end

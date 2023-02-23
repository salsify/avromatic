# frozen_string_literal: true

module Avromatic
  module Model
    module Types
      class AbstractType

        EMPTY_ARRAY = [].freeze
        private_constant :EMPTY_ARRAY

        def value_classes
          raise "#{__method__} must be overridden by #{self.class.name}"
        end

        def input_classes
          value_classes
        end

        def name
          raise "#{__method__} must be overridden by #{self.class.name}"
        end

        def coerce(_input)
          raise "#{__method__} must be overridden by #{self.class.name}"
        end

        def coercible?(input)
          input.nil? || input_classes.any? { |input_class| input.is_a?(input_class) }
        end

        def coerced?(value)
          value.nil? || value_classes.any? { |value_class| value.is_a?(value_class) }
        end

        def matched?(value)
          coerced?(value)
        end

        # Note we use positional args rather than keyword args to reduce
        # memory allocations
        def serialize(_value, _strict)
          raise "#{__method__} must be overridden by #{self.class.name}"
        end

        def referenced_model_classes
          raise "#{__method__} must be overridden by #{self.class.name}"
        end
      end
    end
  end
end

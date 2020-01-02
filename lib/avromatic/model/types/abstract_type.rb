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

        def coercible?(_input)
          raise "#{__method__} must be overridden by #{self.class.name}"
        end

        def coerced?(_value)
          raise "#{__method__} must be overridden by #{self.class.name}"
        end

        def serialize(_value, **)
          raise "#{__method__} must be overridden by #{self.class.name}"
        end

        def referenced_model_classes
          raise "#{__method__} must be overridden by #{self.class.name}"
        end
      end
    end
  end
end

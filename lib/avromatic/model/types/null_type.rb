# frozen_string_literal: true

module Avromatic
  module Model
    module Types
      class NullType
        VALUE_CLASSES = [::NilClass].freeze

        def value_classes
          VALUE_CLASSES
        end

        def name
          'null'
        end

        def coerce(input)
          if input.nil?
            nil
          else
            raise ArgumentError.new("Could not coerce '#{input.inspect}' to #{name}")
          end
        end

        def coercible?(input)
          input.nil?
        end

        alias_method :coerced?, :coercible?

        def serialize(_value, **)
          nil
        end
      end
    end
  end
end

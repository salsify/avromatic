# frozen_string_literal: true

module Avromatic
  module Model
    module Types
      class CustomType
        IDENTITY_PROC = Proc.new { |value| value }

        attr_reader :custom_type_configuration, :value_classes, :default_type

        def initialize(custom_type_configuration:, default_type:)
          @custom_type_configuration = custom_type_configuration
          @default_type = default_type
          @deserializer = custom_type_configuration.deserializer || IDENTITY_PROC
          @serializer = custom_type_configuration.serializer || IDENTITY_PROC
          @value_classes = if custom_type_configuration.value_class
                             [custom_type_configuration.value_class].freeze
                           else
                             default_type.value_classes
                           end
        end

        def name
          custom_type_configuration.value_class ? custom_type_configuration.value_class.name.to_s.freeze : default_type.name
        end

        def coerce(input)
          if input.nil?
            input
          else
            @deserializer.call(input)
          end
        rescue StandardError => e
          raise ArgumentError.new("Could not coerce '#{input.inspect}' to #{name}: #{e.message}")
        end

        def coercible?(input)
          # TODO: Delegate this to optional configuration
          input.nil? || !coerce(input).nil?
        rescue ArgumentError
          false
        end

        def coerced?(value)
          # TODO: Delegate this to optional configuration
          coerce(value) == value
        rescue ArgumentError
          false
        end

        def serialize(value, **)
          @serializer.call(value)
        end
      end
    end
  end
end

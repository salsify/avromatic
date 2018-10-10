module Avromatic
  module Model
    module Validation
      extend ActiveSupport::Concern

      EMPTY_ARRAY = [].freeze

      class << self
        attr_accessor :strategy
      end
      self.strategy = :none

      def self.missing_nested_attributes(attribute:, value_index: nil, value:)
        missing_child_attributes = value.send(:missing_avro_attributes)
        return EMPTY_ARRAY if missing_child_attributes.empty?

        missing_child_attributes.map do |missing_child_attribute|
          if value_index.present?
            "#{attribute}[#{value_index}].#{missing_child_attribute}"
          else
            "#{attribute}.#{missing_child_attribute}"
          end
        end
      end

      included do
        # TODO: Add ActiveModel::Validation that calls missing_avro_attributes
      end

      private

      def avro_validate!
        if Avromatic::Model::Validation.strategy == :active_model
          if self.class.config.mutable
            raise Avromatic::Model::ValidationError.new("#{self.class.name}(#{attributes.inspect}) cannot be serialized: #{errors.full_messages.join(', ')}") unless valid?
          else
            @avro_valid = valid? unless instance_variable_defined?(:@avro_valid)
            raise Avromatic::Model::ValidationError.new("#{self.class.name}(#{attributes.inspect}) cannot be serialized: #{errors.full_messages.join(', ')}") unless @avro_valid
          end
        elsif Avromatic::Model::Validation.strategy == :native
          results = missing_avro_attributes
          raise Avromatic::Model::ValidationError.new("#{self.class.name}(#{attributes.inspect}) cannot be serialized because the following fields are nil: #{results.join(', ')}") if results.present?
        end
      end

      def missing_avro_attributes
        return @missing_attributes if instance_variable_defined?(:@missing_attributes)

        missing_attributes = []
        self.class.attribute_definitions.each_value do |attribute_definition|
          value = send(attribute_definition.name)
          field = attribute_definition.field
          if value.nil? && attribute_definition.required?
            missing_attributes << field.name
          elsif field.type.type_sym == :array
            value.each_with_index do |element, index|
              next unless element.is_a?(Avromatic::Model::Validation)
              missing_attributes.concat(Avromatic::Model::Validation.missing_nested_attributes(attribute: field.name, value_index: index, value: element))
            end
          elsif field.type.type_sym == :map
            value.each do |key, element|
              next unless element.is_a?(Avromatic::Model::Validation)
              missing_attributes.concat(Avromatic::Model::Validation.missing_nested_attributes(attribute: field.name, value_index: key, value: element))
            end
          elsif value.is_a?(Avromatic::Model::Validation)
            missing_attributes.concat(Avromatic::Model::Validation.missing_nested_attributes(attribute: field.name, value: value))
          end
        end

        unless self.class.config.mutable
          @missing_attributes = missing_attributes.deep_freeze
        end

        missing_attributes
      end

      # TODO: Remove this along with ActiveModel validations
      module ClassMethods

        # Returns an array of messages
        def validate_nested_value(value)
          case value
          when Avromatic::Model::Attributes
            validate_record_value(value)
          when Array
            value.flat_map.with_index do |element, index|
              validate_nested_value(element).map do |message|
                "[#{index}]#{message}"
              end
            end
          when Hash
            value.flat_map do |key, map_value|
              # keys for the Avro map type are always strings and do not require
              # validation
              validate_nested_value(map_value).map do |message|
                "['#{key}']#{message}"
              end
            end
          else
            []
          end
        end

        private

        def validate_complex(field_name)
          validate do |instance|
            value = instance.send(field_name)
            messages = self.class.validate_nested_value(value)
            messages.each { |message| instance.errors.add(field_name.to_sym, message) }
          end
        end

        def validate_record_value(record)
          if record && record.invalid?
            record.errors.map do |key, message|
              ".#{key} #{message}".gsub(' .', '.')
            end
          else
            []
          end
        end
      end
    end
  end
end

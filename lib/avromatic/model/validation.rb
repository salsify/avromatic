# frozen_string_literal: true

module Avromatic
  module Model
    module Validation
      extend ActiveSupport::Concern
      include ActiveModel::Validations

      EMPTY_ARRAY = [].freeze

      def self.missing_nested_attributes(attribute, value)
        if value.is_a?(Array)
          results = []
          value.each_with_index do |element, index|
            nested_results = missing_nested_attributes("#{attribute}[#{index}]", element)
            results.concat(nested_results)
          end
          results
        elsif value.is_a?(Hash)
          results = []
          value.each do |key, element|
            nested_results = missing_nested_attributes("#{attribute}['#{key}']", element)
            results.concat(nested_results)
          end
          results
        elsif value.respond_to?(:missing_avro_attributes)
          value.missing_avro_attributes.map do |missing_child_attribute|
            "#{attribute}.#{missing_child_attribute}"
          end
        else
          EMPTY_ARRAY
        end
      end

      included do
        # Support the ActiveModel::Validations interface for backwards compatibility
        validate do |model|
          model.missing_avro_attributes.each do |missing_attribute|
            errors.add(:base, "#{missing_attribute} can't be nil")
          end
        end
      end

      def avro_validate!
        results = missing_avro_attributes
        if results.present?
          raise Avromatic::Model::ValidationError.new("#{self.class.name}(#{attributes.inspect}) cannot be " \
            "serialized because the following attributes are nil: #{results.join(', ')}")
        end
      end

      def missing_avro_attributes
        return @missing_attributes if instance_variable_defined?(:@missing_attributes)

        missing_attributes = []

        self.class.attribute_definitions.each_value do |attribute_definition|
          value = send(attribute_definition.name)
          field = attribute_definition.field
          if value.nil? && field.type.type_sym != :null && attribute_definition.required?
            missing_attributes << field.name
          else
            missing_attributes.concat(Avromatic::Model::Validation.missing_nested_attributes(field.name, value))
          end
        end

        @missing_attributes = missing_attributes.freeze if recursively_immutable?

        missing_attributes
      end
    end
  end
end

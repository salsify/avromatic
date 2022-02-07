# frozen_string_literal: true

module Avromatic
  module Model

    # This concern adds methods for configuration for a model generated from
    # Avro schema(s).
    module Configurable
      extend ActiveSupport::Concern

      # Wraps a reference to a field so we can access both the string and symbolized versions of the name
      # without repeated memory allocations.
      class FieldReference
        attr_reader :name, :name_sym

        def initialize(name)
          @name = -name
          @name_sym = name.to_sym
        end
      end

      included do
        class_attribute :config, instance_accessor: false, instance_predicate: false
      end

      module ClassMethods
        delegate :avro_schema, :value_avro_schema, :key_avro_schema, :mutable?, :immutable?,
                 :avro_schema_subject, :value_avro_schema_subject, :key_avro_schema_subject, to: :config

        def value_avro_field_names
          @value_avro_field_names ||= value_avro_schema.fields.map(&:name).map(&:to_sym).freeze
        end

        def key_avro_field_names
          @key_avro_field_names ||= key_avro_schema.fields.map(&:name).map(&:to_sym).freeze
        end

        def value_avro_field_references
          @value_avro_field_references ||= value_avro_schema.fields.map do |field|
            Avromatic::Model::Configurable::FieldReference.new(field.name)
          end.freeze
        end

        def key_avro_field_references
          @key_avro_field_references ||= key_avro_schema.fields.map do |field|
            Avromatic::Model::Configurable::FieldReference.new(field.name)
          end.freeze
        end

        def value_avro_fields_by_name
          @value_avro_fields_by_name ||= mapped_by_name(value_avro_schema)
        end

        def key_avro_fields_by_name
          @key_avro_fields_by_name ||= mapped_by_name(key_avro_schema)
        end

        def nested_models
          config.nested_models || Avromatic.nested_models
        end

        private

        def mapped_by_name(schema)
          schema.fields.each_with_object(Hash.new) do |field, result|
            result[field.name.to_sym] = field
          end
        end
      end

      delegate :avro_schema, :value_avro_schema, :key_avro_schema,
               :avro_schema_subject, :value_avro_schema_subject, :key_avro_schema_subject,
               :value_avro_field_names, :key_avro_field_names,
               :value_avro_field_references, :key_avro_field_references,
               :mutable?, :immutable?,
               to: :class
    end
  end
end

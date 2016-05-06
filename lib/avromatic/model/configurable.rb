module Avromatic
  module Model

    # This concern adds methods for configuration for a model generated from
    # Avro schema(s).
    module Configurable
      extend ActiveSupport::Concern

      module ClassMethods
        attr_accessor :config
        delegate :avro_schema, :value_avro_schema, :key_avro_schema, to: :config

        def value_avro_field_names
          @value_avro_field_names ||= value_avro_schema.fields.map(&:name).map(&:to_sym).freeze
        end

        def key_avro_field_names
          @key_avro_field_names ||= key_avro_schema.fields.map(&:name).map(&:to_sym).freeze
        end

        def value_avro_fields_by_name
          @value_avro_fields_by_name ||= mapped_by_name(value_avro_schema)
        end

        def key_avro_fields_by_name
          @key_avro_fields_by_name ||= mapped_by_name(key_avro_schema)
        end

        private

        def mapped_by_name(schema)
          schema.fields.each_with_object(Hash.new) do |field, result|
            result[field.name.to_sym] = field
          end
        end
      end

      delegate :avro_schema, :value_avro_schema, :key_avro_schema,
               :value_avro_field_names, :key_avro_field_names,
               to: :class
    end
  end
end

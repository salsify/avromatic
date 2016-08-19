module Avromatic
  module Model

    # This concern adds methods for configuration for a model generated from
    # Avro schema(s).
    module Configurable
      extend ActiveSupport::Concern

      module ClassMethods
        attr_accessor :config
        delegate :avro_schema, :value_avro_schema, :key_avro_schema,
                 :aliases, :inverse_aliases, to: :config

        def inherited(subclass)
          subclass.config = config
        end

        def value_avro_field_names
          @value_avro_field_names ||= extract_field_names(value_avro_schema)
        end

        def key_avro_field_names
          @key_avro_field_names ||= extract_field_names(key_avro_schema)
        end

        def value_avro_fields_by_name
          @value_avro_fields_by_name ||= mapped_by_name(value_avro_schema)
        end

        def key_avro_fields_by_name
          @key_avro_fields_by_name ||= mapped_by_name(key_avro_schema)
        end

        def inverse_aliases
          @inverse_aliases ||= aliases.select { |k, _| k.index('.').nil? }.invert
        end

        private

        def extract_field_names(schema)
          schema.fields.map do |field|
            (aliases[field.name] || field.name).to_sym
          end.freeze
        end

        def mapped_by_name(schema)
          schema.fields.each_with_object(Hash.new) do |field, result|
            result[field.name.to_sym] = field
          end
        end
      end

      delegate :avro_schema, :value_avro_schema, :key_avro_schema,
               :value_avro_field_names, :key_avro_field_names,
               :inverse_aliases,
               to: :class
    end
  end
end

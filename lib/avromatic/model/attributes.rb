require 'active_support/core_ext/object/duplicable'
require 'active_support/time'

module Avromatic
  module Model

    # This module supports defining Virtus attributes for a model based on the
    # fields of Avro schemas.
    module Attributes
      extend ActiveSupport::Concern

      class OptionalFieldError < StandardError
        attr_reader :field

        def initialize(field)
          @field = field
          super("Optional field not allowed: #{field}")
        end
      end

      class AttributeDefinition
        attr_reader :name, :type, :field, :default, :owner
        delegate :serialize, to: :type

        def initialize(owner:, field:, type:)
          @owner = owner
          @field = field
          @type = type
          @name = field.name.to_sym
          @default = if field.default == :no_default
                       nil
                     elsif field.default.duplicable?
                       field.default.dup.deep_freeze
                     else
                       field.default
                     end
        end

        def required?
          FieldHelper.required?(field)
        end

        def coerce(input)
          type.coerce(input)
        rescue StandardError
          raise Avromatic::Model::CoercionError.new("Could not coerce '#{input.inspect}' to #{type.name} when setting #{owner.name}##{name}")
        end
      end

      included do
        class_attribute :attribute_definitions, instance_writer: false
        self.attribute_definitions = {}
      end

      def initialize(data = {})
        valid_keys = []
        attribute_definitions.each do |attribute_name, attribute_definition|
          if data.include?(attribute_name)
            valid_keys << attribute_name
            value = data.fetch(attribute_name)
            _attributes[attribute_name] = attribute_definition.coerce(value)
          elsif data.include?(attribute_name.to_s)
            valid_keys << attribute_name
            value = data[attribute_name.to_s]
            _attributes[attribute_name] = attribute_definition.coerce(value)
          elsif !attributes.include?(attribute_name)
            _attributes[attribute_name] = attribute_definition.default
          end
        end

        unless Avromatic.allow_unknown_attributes || valid_keys.size == data.size
          raise ArgumentError.new("Unexpected attributes for #{self.class.name}: #{(data.keys - valid_keys).map(&:to_s).join(', ')}. Complete arguments: #{data}")
        end
      end

      def to_h
        _attributes.dup
      end

      alias_method :to_hash, :to_h
      alias_method :attributes, :to_h

      private

      def _attributes
        @attributes ||= {}
      end

      module ClassMethods
        def add_avro_fields
          # models are registered in Avromatic.nested_models at this point to
          # ensure that they are available as fields for recursive models.
          register!

          if key_avro_schema
            check_for_field_conflicts!
            begin
              define_avro_attributes(key_avro_schema,
                                     allow_optional: config.allow_optional_key_fields)
            rescue OptionalFieldError => ex
              raise "Optional field '#{ex.field.name}' not allowed in key schema."
            end
          end
          define_avro_attributes(avro_schema)
        end

        private

        def check_for_field_conflicts!
          conflicts =
            (key_avro_field_names & value_avro_field_names).each_with_object([]) do |name, msgs|
              next unless schema_fields_differ?(name)
              msgs << "Field '#{name}' has a different type in each schema: "\
                      "value #{value_avro_fields_by_name[name]}, "\
                      "key #{key_avro_fields_by_name[name]}"
            end

          raise conflicts.join("\n") if conflicts.any?

          conflicts
        end

        # The Avro::Schema::Field#== method is lame. It just compares
        # <field>.type.type_sym.
        def schema_fields_differ?(name)
          key_avro_fields_by_name[name].to_avro !=
            value_avro_fields_by_name[name].to_avro
        end

        def define_avro_attributes(schema, allow_optional: true)
          if schema.type_sym != :record
            raise "Unsupported schema type '#{schema.type_sym}', only 'record' schemas are supported."
          end

          schema.fields.each do |field|
            raise OptionalFieldError.new(field) if !allow_optional && FieldHelper.optional?(field)

            symbolized_field_name = field.name.to_sym
            attribute_definition = AttributeDefinition.new(
              owner: self,
              field: field,
              type: create_type(field)
            )
            attribute_definitions[symbolized_field_name] = attribute_definition

            define_method(field.name) { _attributes[symbolized_field_name] }

            define_method("#{field.name}=") do |value|
              _attributes[symbolized_field_name] = attribute_definitions[symbolized_field_name].coerce(value)
            end

            unless config.mutable # rubocop:disable Style/Next
              private("#{field.name}=")
              define_method(:clone) { self }
              define_method(:dup) { self }
            end
          end
        end

        def create_type(field)
          Avromatic::Model::Types::TypeFactory.create(schema: field.type, nested_models: nested_models)
        end
      end

    end
  end
end

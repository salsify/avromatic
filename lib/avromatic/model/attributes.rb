require 'active_support/core_ext/object/duplicable'
require 'active_support/time'
require 'ice_nine/core_ext/object'
require 'avromatic/model/allowed_type_validator'

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
        attr_reader :name, :type, :field, :default
        delegate :coerce, to: :type

        def initialize(field:, type:)
          @field = field
          @type = type
          @name = field.name.to_sym
          @default = field.default.duplicable? ? field.default.dup.deep_freeze : field.default
        end

        def default?
          default != :no_default
        end
      end

      included do
        class_attribute :attribute_definitions, instance_writer: false
        self.attribute_definitions = {}
      end

      def self.first_union_schema(field_type)
        # TODO: This is a hack until I find a better solution for unions with
        # Virtus. This only handles a union for an optional field with :null
        # and one other type.
        # This hack lives on for now because custom type coercion is not pushed
        # down into unions. This means that custom types can only be optional
        # fields, not members of real unions.
        field_type.schemas.reject { |schema| schema.type_sym == :null }.first
      end

      def initialize(options = {})
        # TODO: Validate keys? We ignore unknown keys
        attribute_definitions.each do |attribute_name, attribute_definition|
          if options.include?(attribute_name)
            value = options.fetch(attribute_name)
            attributes[attribute_name] = attribute_definition.coerce(value)
          elsif options.include?(attribute_name.to_s)
            value = options[attribute_name.to_s]
            attributes[attribute_name] = attribute_definition.coerce(value)
          elsif !attributes.include?(attribute_name) && attribute_definition.default?
            attributes[attribute_name] = attribute_definition.default
          end
        end
      end

      def attributes
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
            raise OptionalFieldError.new(field) if !allow_optional && optional?(field)

            # TODO: Verify the unions with custom types work!

            attribute_definition = AttributeDefinition.new(
              field: field,
              type: create_type(field)
            )
            attribute_definitions[field.name.to_sym] = attribute_definition

            symbolized_field_name = field.name.to_sym
            define_method(field.name) { attributes[symbolized_field_name] }

            define_method("#{field.name}=") do |value|
              attributes[symbolized_field_name] = attribute_definitions[symbolized_field_name].coerce(value)
            end
            private("#{field.name}=") unless config.mutable

            add_validation(attribute_definition)
            add_serializer(field)
          end
        end

        def add_validation(attribute_definition)
          case attribute_definition.field.type.type_sym
          when :enum
            validates(attribute_definition.field.name,
                      inclusion: { in: Set.new(attribute_definition.field.type.symbols.map(&:freeze)).freeze })
          when :fixed
            validates(attribute_definition.field.name, length: { is: attribute_definition.field.type.size })
          when :record, :array, :map, :union
            validate_complex(attribute_definition.field.name)
          else
            add_type_validation(attribute_definition)
          end

          add_required_validation(attribute_definition.field)
        end

        def add_type_validation(attribute_definition)
          validates(attribute_definition.name, allowed_type: attribute_definition.type.value_classes, allow_blank: true)
        end

        def add_required_validation(field)
          if required?(field) && field.default == :no_default
            case field.type.type_sym
            when :array, :map, :boolean
              validates(field.name, exclusion: { in: [nil], message: "can't be nil" })
            else
              validates(field.name, presence: true)
            end
          end
        end

        # An optional field is represented as a union where the first member
        # is null.
        def optional?(field)
          field.type.type_sym == :union &&
            field.type.schemas.first.type_sym == :null
        end

        def required?(field)
          !optional?(field)
        end

        def create_type(field)
          Avromatic::Model::Types::TypeFactory.create(schema: field.type, nested_models: nested_models)
        end

        # TODO: Push this into Type?
        def add_serializer(field)
          # TODO: This won't work for custom types used in maps, arrays or unions
          custom_type = Avromatic.type_registry.fetch(field)
          serializer = custom_type.serializer

          avro_serializer[field.name.to_sym] = serializer if serializer
        end
      end

    end
  end
end

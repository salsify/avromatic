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

      module IdentityCoercer
        def self.call(input)
          input
        end
      end

      module StringCoercer
        def self.call(input)
          input.to_s
        end
      end

      class ArrayCoercer
        def initialize(element_coercer)
          @element_coercer = element_coercer
        end

        def call(input)
          input.map { |element| @element_coercer.call(element) }
        end
      end

      class HashCoercer
        def initialize(key_coercer, value_coercer)
          @key_coercer = key_coercer
          @value_coercer = value_coercer
        end

        def call(input)
          input.each_with_object({}) do |(key, value), result|
            result[@key_coercer.call(key)] = @value_coercer.call(value)
          end
        end
      end

      class AttributeDefinition
        attr_reader :name, :field, :default, :coercer, :value_class
        delegate :name, to: :field

        def initialize(field:, coercer: nil, value_class: )
          @field = field
          @name = field.name.to_sym
          @default = field.default.duplicable? ? field.default.dup.deep_freeze : field.default
          @coercer = coercer
          @value_class = value_class
        end

        def has_default?
          @default != :no_default
        end

        def coerce(value)
          # TODO: Coerce nil?
          coercer.call(value)
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
          elsif !attributes.include?(attribute_name) && attribute_definition.has_default?
            attributes[attribute_name] = attribute_definition.default
          end
        end
      end

      def attributes
        @attributes ||= {}
      end

      private

      def default_for(value)
        value.duplicable? ? value.dup.deep_freeze : value
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

            field_class = avro_field_class(field.type)

            prevent_union_including_custom_type!(field, field_class)

            attribute_definitions[field.name.to_sym] = AttributeDefinition.new(
              field: field,
              coercer: value_coercer(field, field_class),
              value_class: field_class
            )

            symbolized_field_name = field.name.to_sym
            define_method(field.name) { attributes[symbolized_field_name] }

            define_method("#{field.name}=") do |value|
              attributes[symbolized_field_name] = attribute_definitions[symbolized_field_name].coerce(value)
            end
            private("#{field.name}=") unless config.mutable

            add_validation(field, field_class)
            add_serializer(field, field_class)
          end
        end

        def value_coercer(field, value_class)
          # TODO: This encoding of Arrays + Hashes is convoluted
          if value_class.is_a?(Array)
            value_class = value_class.first
            ArrayCoercer.new(value_coercer(field.type.items, value_class))
          elsif value_class.is_a?(Hash)
            # Avro maps always have string keys
            value_class = value_class.first.last
            HashCoercer.new(IdentityCoercer.new, value_coercer(field.type.values, value_class))
          else
            custom_type = Avromatic.type_registry.fetch(field, value_class)
            if custom_type.deserializer
              custom_type.deserializer
            elsif value_class == String
              StringCoercer
            else
              # TODO: What about other types?
              IdentityCoercer
            end
          end
        end

        def add_validation(field, field_class)
          case field.type.type_sym
          when :enum
            validates(field.name,
                      inclusion: { in: Set.new(field.type.symbols.map(&:freeze)).freeze })
          when :fixed
            validates(field.name, length: { is: field.type.size })
          when :record, :array, :map, :union
            validate_complex(field.name)
          else
            add_type_validation(field.name, field_class)
          end

          add_required_validation(field)
        end

        def add_type_validation(name, field_class)
          allowed_types = if field_class == Axiom::Types::Boolean
                            [TrueClass, FalseClass]
                          elsif field_class < Avromatic::Model::Attribute::AbstractTimestamp
                            [Time]
                          else
                            [field_class]
                          end

          validates(name, allowed_type: allowed_types, allow_blank: true)
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

        def avro_field_class(field_type)
          custom_type = Avromatic.type_registry.fetch(field_type)
          return custom_type.value_class if custom_type.value_class

          if field_type.respond_to?(:logical_type)
            value_class = Avromatic::Model::LogicalTypes.value_class(field_type.logical_type)
            return value_class if value_class
          end

          case field_type.type_sym
          when :string, :bytes, :fixed
            String
          when :boolean
            Axiom::Types::Boolean
          when :int, :long
            Integer
          when :float, :double
            Float
          when :enum
            String
          when :null
            NilClass
          when :array
            Array[avro_field_class(field_type.items)]
          when :map
            Hash[String => avro_field_class(field_type.values)]
          when :union
            union_field_class(field_type)
          when :record
            build_nested_model(field_type)
          else
            raise "Unsupported type #{field_type}"
          end
        end

        def union_field_class(field_type)
          null_index = field_type.schemas.index { |schema| schema.type_sym == :null }
          raise 'a null type in a union must be the first member' if null_index && null_index > 0

          field_classes = field_type.schemas.reject { |schema| schema.type_sym == :null }
                            .map { |schema| avro_field_class(schema) }

          if field_classes.size == 1
            field_classes.first
          else
            Avromatic::Model::AttributeType::Union[*field_classes]
          end
        end

        def add_serializer(field, field_class)
          # TODO: This probablly doesn't work properly for arrays or hashes
          custom_type = Avromatic.type_registry.fetch(field, field_class)
          serializer = custom_type.serializer

          avro_serializer[field.name.to_sym] = serializer if serializer
        end

        # TODO: the methods below are temporary until support for custom types
        # as union members are supported.
        def member_uses_custom_type?(field)
          field.type.schemas.any? do |klass|
            Avromatic.type_registry.fetch(klass) != NullCustomType
          end
        end

        def prevent_union_including_custom_type!(field, field_class)
          if field_class.is_a?(Class) &&
            field_class < Avromatic::Model::AttributeType::Union &&
            member_uses_custom_type?(field)

            raise 'custom types within unions are currently unsupported'
          end
        end

      end

    end
  end
end

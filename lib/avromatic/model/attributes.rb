require 'active_support/core_ext/object/duplicable'
require 'ice_nine/core_ext/object'

module Avromatic
  module Model

    # This module supports defining Virtus attributes for a model based on the
    # fields of Avro schemas.
    module Attributes
      extend ActiveSupport::Concern

      def self.first_union_schema(field_type)
        # TODO: This is a hack until I find a better solution for unions with
        # Virtus. This only handles a union for an optional field with :null
        # and one other type.
        # This hack lives on for now because custom type coercion is not pushed
        # down into unions. This means that custom types can only be optional
        # fields, not members of real unions.
        field_type.schemas.reject { |schema| schema.type_sym == :null }.first
      end

      module ClassMethods
        def add_avro_fields
          if key_avro_schema
            check_for_field_conflicts!
            define_avro_attributes(key_avro_schema)
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

        def define_avro_attributes(schema)
          if schema.type_sym != :record
            raise "Unsupported schema type '#{schema.type_sym}', only 'record' schemas are supported."
          end

          schema.fields.each do |field|
            field_class = avro_field_class(field.type)

            attribute(field.name,
                      field_class,
                      avro_field_options(field, field_class))

            add_model(field.name, field_class) if field.type.type_sym == :record

            add_validation(field)
            add_serializer(field, field_class)
          end
        end

        def add_validation(field)
          case field.type.type_sym
          when :enum
            validates(field.name,
                      inclusion: { in: Set.new(field.type.symbols.map(&:freeze)).freeze })
          when :fixed
            validates(field.name, length: { is: field.type.size })
          end

          add_required_validation(field)
        end

        def add_required_validation(field)
          if required?(field) && field.default == :no_default
            validates(field.name, presence: true)
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
            build_model(field_type)
          else
            raise "Unsupported type #{field_type}"
          end
        end

        def union_field_class(field_type)
          field_classes = field_type.schemas.reject { |schema| schema.type_sym == :null }
                            .map { |schema| avro_field_class(schema) }

          if field_classes.size == 1
            field_classes.first
          else
            Avromatic::Model::AttributeType::Union[*field_classes]
          end
        end

        def avro_field_options(field, field_class)
          options = {}

          prevent_union_including_custom_type!(field, field_class)

          custom_type = Avromatic.type_registry.fetch(field, field_class)
          coercer = custom_type.deserializer
          options[:coercer] = coercer if coercer

          # See: https://github.com/dasch/avro_turf/pull/36
          if field.default != :no_default
            options.merge!(default: default_for(field.default), lazy: true)
          end

          options
        end

        def add_serializer(field, field_class)
          prevent_union_including_custom_type!(field, field_class)

          custom_type = Avromatic.type_registry.fetch(field, field_class)
          serializer = custom_type.serializer

          avro_serializer[field.name.to_sym] = serializer if serializer
        end

        def default_for(value)
          value.duplicable? ? value.dup.deep_freeze : value
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

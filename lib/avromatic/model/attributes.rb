require 'active_support/core_ext/object/duplicable'
require 'ice_nine/core_ext/object'

module Avromatic
  module Model

    # This module supports defining Virtus attributes for a model based on the
    # fields of Avro schemas.
    module Attributes
      extend ActiveSupport::Concern

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
          (key_avro_field_names & value_avro_field_names).each_with_object([]) do |name, conflicts|
              if schema_fields_differ?(name)
                conflicts << "Field '#{name}' has a different type in each schema: "\
                             "value #{value_avro_fields_by_name[name]}, "\
                             "key #{key_avro_fields_by_name[name]}"
              end
          end.tap do |conflicts|
            raise conflicts.join("\n") if conflicts.any?
          end
        end

        # The Avro::Schema::Field#== method is lame. It just compares
        # <field>.type.type_sym.
        def schema_fields_differ?(name)
          key_avro_fields_by_name[name].to_avro !=
            value_avro_fields_by_name[name].to_avro
        end

        def define_avro_attributes(schema)
          schema.fields.each do |field|
            field_class = avro_field_class(field.type)

            attribute(field.name,
                      field_class,
                      avro_field_options(field))

            add_validation(field)
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
          if required?(field) && field.default.nil?
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

        # TODO: Introduce some kind of TypeRegistry?
        def fetch_custom_type(field_type)
          if field_type.respond_to?(:fullname)
            Avromatic.custom_types[field_type.fullname]
          end
        end

        def avro_field_class(field_type)
          custom_type = fetch_custom_type(field_type)
          if custom_type
            value_class = custom_type.value_class
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
            # TODO: This should add the generated model to a module.
            # A hash of generated models should be kept by name for reuse.
            Class.new do
              include Avromatic::Model.build(schema: field_type)
            end
          else
            raise "Unsupported type #{field_type}"
          end
        end

        def union_field_class(field_type)
          # TODO: This is a hack until I find a better solution for unions with
          # Virtus. This only handles a union for a optional field with :null
          # and one other type.
          schemas = field_type.schemas.reject { |schema| schema.type_sym == :null }
          raise "Only the union of null with one other type is supported #{field_type}" if schemas.size > 1
          avro_field_class(schemas.first)
        end

        def avro_field_options(field)
          options = {}

          custom_type = fetch_custom_type(field.type)
          if custom_type
            coercer = if custom_type.from_avro
                        custom_type.from_avro
                      elsif custom_type.value_class && custom_type.value_class.respond_to?(:from_avro)
                        custom_type.value_class.method(:from_avro).to_proc
                      end
            coder = if custom_type.to_avro
                      custom_type.to_avro
                    elsif custom_type.value_class && custom_type.value_class.respond_to?(:to_avro)
                      custom_type.value_class.method(:to_avro).to_proc
                    end
            avro_coder[field.name.to_sym] = coder if coder

            options[:coercer] = coercer if coercer
          end

          if field.default
            options.merge!(default: default_for(field.default), lazy: true)
          end

          options
        end

        def default_for(value)
          value.duplicable? ? value.dup.deep_freeze : value
        end
      end

    end
  end
end

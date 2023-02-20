# frozen_string_literal: true

require 'avromatic/model/types/array_type'
require 'avromatic/model/types/boolean_type'
require 'avromatic/model/types/custom_type'
require 'avromatic/model/types/date_type'
require 'avromatic/model/types/decimal_type'
require 'avromatic/model/types/enum_type'
require 'avromatic/model/types/fixed_type'
require 'avromatic/model/types/float_type'
require 'avromatic/model/types/integer_type'
require 'avromatic/model/types/big_int_type'
require 'avromatic/model/types/map_type'
require 'avromatic/model/types/null_type'
require 'avromatic/model/types/record_type'
require 'avromatic/model/types/string_type'
require 'avromatic/model/types/timestamp_micros_type'
require 'avromatic/model/types/timestamp_millis_type'
require 'avromatic/model/types/union_type'

module Avromatic
  module Model
    module Types
      module TypeFactory
        extend self

        SINGLETON_TYPES = {
          'date' => Avromatic::Model::Types::DateType.new,
          'timestamp-micros' => Avromatic::Model::Types::TimestampMicrosType.new,
          'timestamp-millis' => Avromatic::Model::Types::TimestampMillisType.new,
          'string' => Avromatic::Model::Types::StringType.new,
          'bytes' => Avromatic::Model::Types::StringType.new,
          'boolean' => Avromatic::Model::Types::BooleanType.new,
          'int' => Avromatic::Model::Types::IntegerType.new,
          'long' => Avromatic::Model::Types::BigIntType.new,
          'float' => Avromatic::Model::Types::FloatType.new,
          'double' => Avromatic::Model::Types::FloatType.new,
          'null' => Avromatic::Model::Types::NullType.new
        }.deep_freeze

        def create(schema:, nested_models:, use_custom_types: true)
          if use_custom_types && Avromatic.custom_type_registry.registered?(schema)
            custom_type_configuration = Avromatic.custom_type_registry.fetch(schema)
            default_type = create(
              schema: schema,
              nested_models: nested_models,
              use_custom_types: false
            )
            Avromatic::Model::Types::CustomType.new(
              custom_type_configuration: custom_type_configuration,
              default_type: default_type
            )
          elsif schema.respond_to?(:logical_type) && SINGLETON_TYPES.include?(schema.logical_type)
            SINGLETON_TYPES.fetch(schema.logical_type)
          elsif schema.respond_to?(:logical_type) && schema.logical_type == 'decimal' &&
                Avromatic.allow_decimal_logical_type
            Avromatic::Model::Types::DecimalType.new(precision: schema.precision, scale: schema.scale || 0)
          elsif SINGLETON_TYPES.include?(schema.type)
            SINGLETON_TYPES.fetch(schema.type)
          else
            case schema.type_sym
            when :fixed
              Avromatic::Model::Types::FixedType.new(schema.size)
            when :enum
              Avromatic::Model::Types::EnumType.new(schema.symbols)
            when :array
              value_type = create(schema: schema.items, nested_models: nested_models,
                                  use_custom_types: use_custom_types)
              Avromatic::Model::Types::ArrayType.new(value_type: value_type)
            when :map
              value_type = create(schema: schema.values, nested_models: nested_models,
                                  use_custom_types: use_custom_types)
              Avromatic::Model::Types::MapType.new(
                key_type: Avromatic::Model::Types::StringType.new,
                value_type: value_type
              )
            when :union
              null_index = schema.schemas.index { |member_schema| member_schema.type_sym == :null }
              raise 'a null type in a union must be the first member' if null_index && null_index > 0

              member_schemas = schema.schemas.reject { |member_schema| member_schema.type_sym == :null }
              if member_schemas.size == 1
                create(schema: member_schemas.first, nested_models: nested_models)
              else
                member_types = member_schemas.map do |member_schema|
                  create(schema: member_schema, nested_models: nested_models, use_custom_types: use_custom_types)
                end
                Avromatic::Model::Types::UnionType.new(member_types: member_types)
              end
            when :record
              record_class = build_nested_model(schema: schema, nested_models: nested_models)
              Avromatic::Model::Types::RecordType.new(record_class: record_class)
            else
              raise ArgumentError.new("Unsupported type #{schema.type_sym}")
            end
          end
        end

        private

        def build_nested_model(schema:, nested_models:)
          fullname = nested_models.remove_prefix(schema.fullname)

          if nested_models.registered?(fullname)
            nested_model = nested_models[fullname]
            unless schema_fingerprint(schema) == schema_fingerprint(nested_model.avro_schema)
              raise "The #{nested_model.name} model is already registered with an incompatible version of the " \
                    "#{schema.fullname} schema"
            end

            nested_model
          else
            Avromatic::Model.model(schema: schema, nested_models: nested_models)
          end
        end

        def schema_fingerprint(schema)
          if schema.respond_to?(:sha256_resolution_fingerprint)
            schema.sha256_resolution_fingerprint
          else
            schema.sha256_fingerprint
          end
        end
      end
    end
  end
end

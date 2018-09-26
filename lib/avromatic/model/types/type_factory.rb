module Avromatic
  module Model
    module Types
      module TypeFactory
        extend self

        def create(schema:, nested_models:, use_custom_types: true)
          if use_custom_types && Avromatic.type_registry.registered?(schema)
            custom_type = Avromatic.type_registry.fetch(schema)
            default_value_classes = create(
              schema: schema,
              nested_models: nested_models,
              use_custom_types: false
            ).value_classes
            return Avromatic::Model::Types::CustomTypeAdapter.new(
              custom_type: custom_type,
              default_value_classes: default_value_classes
            )
          elsif schema.respond_to?(:logical_type)
            case schema.logical_type
            when 'date'
              return Avromatic::Model::Types::DateType.new
            when 'timestamp-micros'
              return Avromatic::Model::Types::TimestampMicrosType.new
            when 'timestamp-millis'
              return Avromatic::Model::Types::TimestampMillisType.new
            end
          end

          case schema.type_sym
          when :string, :bytes, :fixed
            Avromatic::Model::Types::StringType.new
          when :boolean
            Avromatic::Model::Types::BooleanType.new
          when :int, :long
            Avromatic::Model::Types::IntegerType.new
          when :float, :double
            Avromatic::Model::Types::FloatType.new
          when :enum
            # TODO: Create enum type?
            Avromatic::Model::Types::StringType.new
          when :null
            Avromatic::Model::Types::NullType.new
          when :array
            value_type = create(schema: schema.items, nested_models: nested_models, use_custom_types: use_custom_types)
            Avromatic::Model::Types::ArrayType.new(value_type: value_type)
          when :map
            value_type = create(schema: schema.values, nested_models: nested_models, use_custom_types: use_custom_types)
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
            raise "Unsupported type #{schema.type_sym}"
          end
        end

        private

        # TODO: Copied from NestedModels
        def build_nested_model(schema:, nested_models:)
          fullname = nested_models.remove_prefix(schema.fullname)

          if nested_models.registered?(fullname)
            nested_models[fullname]
          else
            Avromatic::Model.model(schema: schema,
                                   nested_models: nested_models)
          end
        end
      end
    end
  end
end

module Avromatic
  module Model
    module AttributeType
      module TypeFactory
        extend self

        def create(schema:, nested_models:)
          if Avromatic.type_registry.registered?(schema)
            custom_type = Avromatic.type_registry.fetch(schema)
            return Avromatic::Model::AttributeType::CustomTypeAdapter.new(custom_type: custom_type)
          elsif schema.respond_to?(:logical_type)
            case schema.logical_type
            when 'date'
              return Avromatic::Model::AttributeType::DateType.new
            when 'timestamp-micros'
              return Avromatic::Model::AttributeType::TimestampMicrosType.new
            when 'timestamp-millis'
              return Avromatic::Model::AttributeType::TimestampMillisType
            end
          end

          case schema.type_sym
          when :string, :bytes, :fixed
            Avromatic::Model::AttributeType::StringType.new
          when :boolean
            Avromatic::Model::AttributeType::BooleanType.new
          when :int, :long
            Avromatic::Model::AttributeType::IntegerType.new
          when :float, :double
            Avromatic::Model::AttributeType::FloatType.new
          when :enum
            # TODO: Create enum type?
            Avromatic::Model::AttributeType::StringType.new
          when :null
            Avromatic::Model::AttributeType::NullType.new
          when :array
            value_type = create(schema: schema.items, nested_models: nested_models)
            Avromatic::Model::AttributeType::ArrayType.new(value_type: value_type)
          when :map
            value_type = create(schema: schema.values, nested_models: nested_models)
            Avromatic::Model::AttributeType::MapType.new(
              key_type: Avromatic::Model::AttributeType::StringType.new,
              value_type: value_type
            )
          when :union
            null_index = schema.schemas.index { |schema| schema.type_sym == :null }
            raise 'a null type in a union must be the first member' if null_index && null_index > 0

            member_schemas = schema.schemas.reject { |schema| schema.type_sym == :null }
            if member_schemas.size == 1
              create(schema: member_schemas.first, nested_models: nested_models)
            else
              member_types = member_schemas.map do |member_schema|
                create(schema: member_schema, nested_models: nested_models)
              end
              Avromatic::Model::AttributeType::UnionType.new(member_types: member_types)
            end
          when :record
            record_class = build_nested_model(schema: schema, nested_models: nested_models)
            Avromatic::Model::AttributeType::RecordType.new(record_class: record_class)
          else
            raise "Unsupported type #{schema.type_sym}"
          end
        end

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

module Avromatic
  module Model

    # This class holds configuration for a model built from Avro schema(s).
    class Configuration

      attr_reader :avro_schema, :key_avro_schema, :nested_models, :mutable,
                  :allow_optional_key_fields
      delegate :schema_store, to: Avromatic

      # Either schema(_name) or value_schema(_name), but not both, must be
      # specified.
      #
      # @param options [Hash]
      # @option options [Avro::Schema] :schema
      # @option options [String, Symbol] :schema_name
      # @option options [Avro::Schema] :value_schema
      # @option options [String, Symbol] :value_schema_name
      # @option options [Avro::Schema] :key_schema
      # @option options [String, Symbol] :key_schema_name
      # @option options [Avromatic::ModelRegistry] :nested_models
      # @option options [Boolean] :mutable, default false
      # @option options [Boolean] :allow_optional_key_fields, default false
      def initialize(**options)
        @avro_schema = find_avro_schema(**options)
        raise ArgumentError.new('value_schema(_name) or schema(_name) must be specified') unless avro_schema
        @key_avro_schema = find_schema_by_option(:key_schema, **options)
        @nested_models = options[:nested_models]
        @mutable = options.fetch(:mutable, false)
        @allow_optional_key_fields = options.fetch(:allow_optional_key_fields, false)
      end

      alias_method :value_avro_schema, :avro_schema

      private

      def find_avro_schema(**options)
        if (options[:value_schema] || options[:value_schema_name]) &&
          (options[:schema] || options[:schema_name])
          raise ArgumentError.new('Only one of value_schema(_name) and schema(_name) can be specified')
        end
        find_schema_by_option(:value_schema, **options) || find_schema_by_option(:schema, **options)
      end

      def find_schema_by_option(option_name, **options)
        schema_name_option = :"#{option_name}_name"
        options[option_name] ||
          (options[schema_name_option] && schema_store.find(options[schema_name_option]))
      end
    end
  end
end

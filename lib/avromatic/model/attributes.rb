# frozen_string_literal: true

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
        attr_reader :name, :name_string, :setter_name, :type, :field, :default, :owner
        delegate :serialize, to: :type

        def initialize(owner:, field:, type:)
          @owner = owner
          @field = field
          @required = FieldHelper.required?(field)
          @type = type
          @values_immutable = type.referenced_model_classes.all?(&:recursively_immutable?)
          @name = field.name.to_sym
          @name_string = field.name.to_s.dup.freeze
          @setter_name = "#{field.name}=".to_sym
          @default = if field.default == :no_default
                       nil
                     elsif field.default.duplicable?
                       field.default.dup.deep_freeze
                     else
                       field.default
                     end
        end

        def required?
          @required
        end

        def values_immutable?
          @values_immutable
        end

        def coerce(input)
          type.coerce(input)
        rescue Avromatic::Model::UnknownAttributeError => e
          raise Avromatic::Model::CoercionError.new("Value for #{owner.name}##{name} could not be coerced to a #{type.name} " \
            "because the following unexpected attributes were provided: #{e.unknown_attributes.join(', ')}. " \
            "Only the following attributes are allowed: #{e.allowed_attributes.join(', ')}. " \
            "Provided argument: #{input.inspect}")
        rescue StandardError
          if type.input_classes && type.input_classes.none? { |input_class| input.is_a?(input_class) }
            raise Avromatic::Model::CoercionError.new("Value for #{owner.name}##{name} could not be coerced to a #{type.name} " \
              "because a #{input.class.name} was provided but expected a #{type.input_classes.map(&:name).to_sentence(two_words_connector: ' or ', last_word_connector: ', or ')}. " \
              "Provided argument: #{input.inspect}")
          elsif input.is_a?(Hash) && type.is_a?(Avromatic::Model::Types::UnionType)
            raise Avromatic::Model::CoercionError.new("Value for #{owner.name}##{name} could not be coerced to a #{type.name} " \
              "because no union member type matches the provided attributes: #{input.inspect}")
          else
            raise Avromatic::Model::CoercionError.new("Value for #{owner.name}##{name} could not be coerced to a #{type.name}. " \
              "Provided argument: #{input.inspect}")
          end
        end
      end

      included do
        class_attribute :attribute_definitions, instance_writer: false
        self.attribute_definitions = {}

        delegate :recursively_immutable?, to: :class
      end

      def initialize(data = {})
        super()

        num_valid_keys = 0
        # Prevalidation checks to see if all required attributes are recursively present for immutable
        # subtrees of models to avoid an additional validation traversal of the model tree during
        # serialization. Model classes that default any attributes after this constructor is invoked
        # will not be lazily initialized.
        @prevalidated = immutable?
        attribute_definitions.each do |attribute_name, attribute_definition|
          value = if data.include?(attribute_name)
                    num_valid_keys += 1
                    send(attribute_definition.setter_name, data.fetch(attribute_name))
                  elsif data.include?(attribute_definition.name_string)
                    num_valid_keys += 1
                    send(attribute_definition.setter_name, data[attribute_definition.name_string])
                  elsif !_attributes.include?(attribute_name)
                    send(attribute_definition.setter_name, attribute_definition.default)
                  else
                    _attributes[attribute_name]
                  end

          if @prevalidated && attribute_definition.required? && !prevalidated_attribute_value?(value)
            @prevalidated = false
          end
        end

        unless Avromatic.allow_unknown_attributes || num_valid_keys == data.size
          unknown_attributes = (data.keys.map(&:to_s) - _attributes.keys.map(&:to_s)).sort
          allowed_attributes = attribute_definitions.keys.map(&:to_s).sort
          message = "Unexpected arguments for #{self.class.name}#initialize: #{unknown_attributes.join(', ')}. " \
            "Only the following arguments are allowed: #{allowed_attributes.join(', ')}. Provided arguments: #{data.inspect}"
          raise Avromatic::Model::UnknownAttributeError.new(message, unknown_attributes: unknown_attributes,
                                                            allowed_attributes: allowed_attributes)
        end
      end

      def to_h
        _attributes.dup
      end

      alias_method :to_hash, :to_h
      alias_method :attributes, :to_h

      protected

      # Returns true if the model was prevalidated in the constructor.
      def prevalidated?
        @prevalidated
      end

      private

      def prevalidated_attribute_value?(value)
        if value.nil?
          false
        elsif value.is_a?(Avromatic::Model::Attributes)
          value.prevalidated?
        elsif value.is_a?(::Array)
          value.all? do |nested_value|
            prevalidated_attribute_value?(nested_value)
          end
        elsif value.is_a?(::Hash)
          value.each_value.all? do |nested_value|
            prevalidated_attribute_value?(nested_value)
          end
        else
          true
        end
      end

      def _attributes
        @attributes ||= {}
      end

      module ClassMethods
        def add_avro_fields(generated_methods_module)
          # models are registered in Avromatic.nested_models at this point to
          # ensure that they are available as fields for recursive models.
          register!

          if key_avro_schema
            check_for_field_conflicts!
            begin
              define_avro_attributes(key_avro_schema, generated_methods_module,
                                     allow_optional: config.allow_optional_key_fields)
            rescue OptionalFieldError => ex
              raise "Optional field '#{ex.field.name}' not allowed in key schema."
            end
          end
          define_avro_attributes(avro_schema, generated_methods_module)
        end

        def recursively_immutable?
          return @recursively_immutable if defined?(@recursively_immutable)

          @recursively_immutable = immutable? && attribute_definitions.each_value.all?(&:values_immutable?)
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

        def define_avro_attributes(schema, generated_methods_module, allow_optional: true)
          if schema.type_sym != :record
            raise "Unsupported schema type '#{schema.type_sym}', only 'record' schemas are supported."
          end

          schema.fields.each do |field|
            raise OptionalFieldError.new(field) if !allow_optional && FieldHelper.optional?(field)

            symbolized_field_name = field.name.to_sym
            attribute_definition = AttributeDefinition.new(
              owner: self,
              field: field,
              type: Avromatic::Model::Types::TypeFactory.create(schema: field.type, nested_models: nested_models)
            )
            attribute_definitions[symbolized_field_name] = attribute_definition

            # Add all generated methods to a module so they can be overridden
            generated_methods_module.send(:define_method, field.name) { _attributes[symbolized_field_name] }
            generated_methods_module.send(:define_method, "#{field.name}?") { !!_attributes[symbolized_field_name] } if FieldHelper.boolean?(field)

            generated_methods_module.send(:define_method, "#{field.name}=") do |value|
              _attributes[symbolized_field_name] = attribute_definition.coerce(value)
            end

            unless mutable? # rubocop:disable Style/Next
              generated_methods_module.send(:private, "#{field.name}=")
              generated_methods_module.send(:define_method, :clone) { self }
              generated_methods_module.send(:define_method, :dup) { self }
            end
          end
        end
      end

    end
  end
end

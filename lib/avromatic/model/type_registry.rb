require 'avromatic/model/custom_type'

module Avromatic
  module Model
    class TypeRegistry

      delegate :clear, to: :custom_types

      def initialize
        @custom_types = {}
      end

      # @param fullname [String] The fullname of the Avro type.
      # @param value_class [Class] Optional class to use for the attribute.
      #   If unspecified then the default class for the Avro field is used.
      # @block If a block is specified then the CustomType is yielded for
      #   additional configuration.
      def register_type(fullname, value_class = nil)
        custom_types[fullname.to_s] = Avromatic::Model::CustomType.new(value_class).tap do |type|
          yield(type) if block_given?
        end
      end

      def registered?(object)
        field_type = object.is_a?(Avro::Schema::Field) ? object.type : object
        # TODO: Handle unions
        custom_types.include?(field_type.fullname) if field_type.is_a?(Avro::Schema::NamedSchema)
      end

      # @object [Avro::Schema] Custom type may be fetched based on a Avro field
      #   or schema. If there is no custom type, then NullCustomType is returned.
      # @field_class [Object] Value class that has been determined for a field.
      def fetch(object, field_class = nil)
        field_type = object.is_a?(Avro::Schema::Field) ? object.type : object

        if field_class && field_type.type_sym == :union && !union_attribute?(field_class)
          field_type = Avromatic::Model::Attributes.first_union_schema(field_type)
        end

        fullname = field_type.fullname if field_type.is_a?(Avro::Schema::NamedSchema)
        custom_types.fetch(fullname, NullCustomType)
      end

      private

      attr_reader :custom_types

      # The type that is used to define a Virtus attribute may be a Class or
      # for an array or map field it may be an instance of an Array or Hash.
      # This method safely checks if a Union class has been selected.
      def union_attribute?(attribute_type)
        attribute_type.is_a?(Class) && attribute_type < Avromatic::Model::AttributeType::UnionType
      end
    end
  end
end

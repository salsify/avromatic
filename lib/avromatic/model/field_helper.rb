# frozen_string_literal: true

module Avromatic
  module Model
    module FieldHelper
      extend self

      # An optional field is represented as a union where the first member
      # is null.
      def optional?(field)
        field.type.type_sym == :union &&
          field.type.schemas.first.type_sym == :null
      end

      def required?(field)
        !optional?(field)
      end

      def nullable?(field)
        optional?(field) || field.type.type_sym == :null
      end

      def boolean?(field)
        field.type.type_sym == :boolean ||
          (optional?(field) && field.type.schemas.last.type_sym == :boolean)
      end
    end
  end
end

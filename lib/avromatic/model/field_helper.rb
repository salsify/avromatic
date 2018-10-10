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
    end
  end
end

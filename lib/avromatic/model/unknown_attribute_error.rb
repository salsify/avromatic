# frozen_string_literal: true

module Avromatic
  module Model
    class UnknownAttributeError < StandardError
      attr_reader :attributes

      def initialize(message, attributes)
        super(message)
        @attributes = attributes
      end
    end
  end
end

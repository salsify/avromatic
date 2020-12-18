# frozen_string_literal: true

module Avromatic
  module Model
    class ValidationError < StandardError
      attr_accessor :missing_attributes

      def initialize(message, missing_attributes)
        super(message)
        @missing_attributes = missing_attributes
      end
    end
  end
end

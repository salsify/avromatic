# frozen_string_literal: true

module Avromatic
  module Model
    class UnknownAttributeError < StandardError
      attr_reader :unknown_attributes, :allowed_attributes

      def initialize(message, unknown_attributes:, allowed_attributes:)
        super(message)
        @unknown_attributes = unknown_attributes
        @allowed_attributes = allowed_attributes
      end
    end
  end
end

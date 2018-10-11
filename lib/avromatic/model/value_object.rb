# frozen_string_literal: true

module Avromatic
  module Model
    # This module is used to override the comparisons defined by
    # Virtus::Equalizer which is pulled in by Virtus::ValueObject.
    module ValueObject
      def eql?(other)
        other.instance_of?(self.class) && attributes == other.attributes
      end
      alias_method :==, :eql?

      def hash
        attributes.hash
      end

      def inspect
        "#<#{self.class.name} #{attributes.map { |k, v| "#{k}: #{v.inspect}" }.join(', ')}>"
      end

      def to_s
        format('#<%<class_name>s:0x00%<identifier>x>', class_name: self.class.name, identifier: object_id.abs * 2)
      end
    end
  end
end

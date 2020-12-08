# frozen_string_literal: true

module Avromatic
  module IO
    class UnionDatum
      attr_reader :member_index, :datum

      def initialize(member_index, datum)
        @member_index = member_index
        @datum = datum
      end

      def ==(other)
        other.is_a?(Avromatic::IO::UnionDatum) &&
          member_index == other.member_index &&
          datum == other.datum
      end
      alias_method :eql?, :==

      def hash
        31 * datum.hash + member_index
      end
    end
  end
end

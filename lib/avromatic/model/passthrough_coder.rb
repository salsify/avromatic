module Avromatic
  module Model
    # This trivial coder simply returns the value provided.
    module PassthroughCoder
      def self.call(value)
        value
      end
    end
  end
end

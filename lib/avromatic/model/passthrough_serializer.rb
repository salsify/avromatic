module Avromatic
  module Model
    # This trivial serializer simply returns the value provided.
    module PassthroughSerializer
      def self.call(value)
        value
      end
    end
  end
end

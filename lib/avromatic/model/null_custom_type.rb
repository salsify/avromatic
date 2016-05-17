module Avromatic
  module Model

    # This module is used to implement the null object pattern for a CustomType.
    module NullCustomType
      class << self
        def value_class
          nil
        end

        def deserializer
          nil
        end

        def serializer
          nil
        end
      end
    end
  end
end

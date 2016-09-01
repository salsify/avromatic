module Avromatic
  module Model
    module AttributeType

      # This class is used as the base class for Virtus attributes that
      # represent unions. A subclass is generated using Union[*types]. This
      # subclass is specified when defining a Virtus attribute.
      class Union
        class << self
          attr_reader :types

          protected

          attr_writer :types
        end

        # Factory method to define Union types with the specified list of
        # types (classes).
        def self.[](*types)
          Class.new(self).tap do |klass|
            klass.types = types
            # See https://github.com/solnic/virtus/issues/284#issuecomment-56405137
            Axiom::Types::Object.new { primitive(klass) }
          end
        end
      end
    end
  end
end

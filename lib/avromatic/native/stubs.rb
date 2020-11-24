module Avromatic
  module Native
    # Used by native code to return attribute data to mixins, such as validation,
    # in order to maintain compatibility.
    module Stubs
      AttributeDefinition = Struct.new(:name, :field, :required?)
      Field = Struct.new(:name, :type)
      Type = Struct.new(:type_sym)
    end
  end
end

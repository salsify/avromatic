require 'spec_helper'

describe VirtusValueObjectAllowedWriterMethodsMemoization do

  describe "#allowed_writer_methods" do
    let(:test_class) { Avromatic::Model.model(value_schema_name: 'test.encode_value') }
    let(:instance1) { test_class.new({ str1: 'a', str2: 'b' }) }
    let(:instance2) { test_class.new({ str1: 'c', str2: 'd' }) }

    it "shares the return value across instances" do
      expect(instance1.allowed_writer_methods.object_id).to eq(instance2.allowed_writer_methods.object_id)
    end
  end
end

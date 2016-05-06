require 'spec_helper'

describe Avromatic::Model do

  describe ".build" do
    it "delegates to Avromatic::Model::Builder" do
      args = Hash.new
      builder = instance_double(Avromatic::Model::Builder, mod: Module.new)
      allow(Avromatic::Model::Builder).to receive(:new).and_return(builder)
      described_class.build(args)
      expect(Avromatic::Model::Builder).to have_received(:new)
        .with(args)
      expect(builder).to have_received(:mod)
    end
  end

  describe ".model" do
    it "delegates to Avromatic::Model::Builder" do
      args = Hash.new
      allow(Avromatic::Model::Builder).to receive(:model)
      described_class.model(args)
      expect(Avromatic::Model::Builder).to have_received(:model).with(args)
    end
  end
end

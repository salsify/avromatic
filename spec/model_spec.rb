require 'spec_helper'

describe SalsifyAvro::Model do

  describe ".build" do
    it "delegates to SalsifyAvro::Model::Builder" do
      args = Hash.new
      builder = instance_double(SalsifyAvro::Model::Builder, mod: Module.new)
      allow(SalsifyAvro::Model::Builder).to receive(:new).and_return(builder)
      described_class.build(args)
      expect(SalsifyAvro::Model::Builder).to have_received(:new)
        .with(args)
      expect(builder).to have_received(:mod)
    end
  end

  describe ".model" do
    it "delegates to SalsifyAvro::Model::Builder" do
      args = Hash.new
      allow(SalsifyAvro::Model::Builder).to receive(:model)
      described_class.model(args)
      expect(SalsifyAvro::Model::Builder).to have_received(:model).with(args)
    end
  end
end

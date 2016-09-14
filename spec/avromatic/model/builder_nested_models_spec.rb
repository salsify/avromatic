describe Avromatic::Model::Builder, 'nested_models' do
  let(:schema) do
    Avro::Builder.build_schema do
      record :rec do
        required :sub, :record, type_name: :sub_rec, type_namespace: 'test.bar' do
          required :s, :string
        end
        optional :opt_sub, :sub_rec
      end
    end
  end
  let(:schema2) do
    Avro::Builder.build_schema do
      record :rec2 do
        required :same, :record, type_name: :sub_rec, type_namespace: 'test.bar' do
          required :s, :string
        end
      end
    end
  end

  context "when the nested_models option is not specified" do
    let(:model) { described_class.model(schema: schema) }
    let(:model2) { described_class.model(schema: schema2) }

    it "registers nested models in the Avromatic registry" do
      expect(model.nested_models['test.bar.sub_rec'])
        .to equal(Avromatic.nested_models['test.bar.sub_rec'])
    end

    it "reuses nested models for multiple fields" do
      expect(model.attribute_set['sub'].primitive)
        .to equal(model.attribute_set['opt_sub'].primitive)
    end

    it "reuses nested models for multiple models" do
      expect(model.attribute_set['sub'].primitive)
        .to equal(model2.attribute_set['same'].primitive)
    end
  end

  context "when the nested_models option is specified" do
    let(:registry) { Avromatic::ModelRegistry.new }
    let(:model) { described_class.model(schema: schema, nested_models: registry) }
    let(:registry2) { Avromatic::ModelRegistry.new }
    let(:model2) { described_class.model(schema: schema2, nested_models: registry2) }

    it "uses the specified registry for nested models" do
      aggregate_failures do
        expect(model.nested_models['test.bar.sub_rec']).to equal(model.attribute_set['sub'].primitive)
        expect(Avromatic.nested_models.registered?('test.bar.sub_rec')).to eql(false)
      end
    end

    it "reuses nested models for multiple fields" do
      expect(model.attribute_set['sub'].primitive)
        .to equal(model.attribute_set['opt_sub'].primitive)
    end

    it "does not reuse nested models for models with different registries" do
      expect(model.attribute_set['sub'].primitive)
        .not_to equal(model2.attribute_set['same'].primitive)
    end
  end
end

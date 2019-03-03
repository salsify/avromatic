# frozen_string_literal: true

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
  end
end

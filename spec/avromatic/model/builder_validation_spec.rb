describe Avromatic::Model::Builder, 'validation' do
  let(:schema) { schema_store.find(schema_name) }
  let(:test_class) do
    described_class.model(schema_name: schema_name)
  end
  let(:attribute_names) do
    test_class.attribute_set.map(&:name).map(&:to_s)
  end

  context "fixed" do
    let(:schema_name) { 'test.named_fields' }

    it "validates the length of a fixed field" do
      instance = test_class.new(f: '12345678')
      expect(instance).to be_invalid
      expect(instance.errors[:f]).to include('is the wrong length (should be 7 characters)')
    end
  end

  context "enum" do
    let(:schema_name) { 'test.named_fields' }

    it "validates that an enum is a valid symbol" do
      instance = test_class.new(e: :C)
      expect(instance).to be_invalid
      expect(instance.errors[:e]).to include('is not included in the list')
    end
  end

  context "required" do
    context "primitive types" do
      let(:schema_name) { 'test.primitive_types' }

      it "validates that required fields must be present" do
        instance = test_class.new
        aggregate_failures do
          expect(instance).to be_invalid
          expect(instance.errors[:s]).to include("can't be blank")
          expect(instance.errors[:tf]).to include("can't be nil")
          expect(instance.errors.keys.map(&:to_s)).to match_array(attribute_names)
        end
      end

      context "boolean" do
        it "allows a boolean field to be false" do
          instance = test_class.new(tf: false)
          expect(instance.errors.keys).not_to include(:tf)
        end
      end
    end

    context "array" do
      let(:schema) do
        Avro::Builder.build_schema do
          record :has_array do
            required :a, :array, items: :int
          end
        end
      end
      let(:test_class) { described_class.model(schema: schema) }

      it "validates that a required array is not nil" do
        pending 'Virtus coerces nil values to an empty array'
        instance = test_class.new(a: nil)
        expect(instance).to be_invalid
        expect(instance.errors[:a]).to include("can't be nil")
      end

      it "allows a required array to be empty" do
        instance = test_class.new(a: [])
        expect(instance).to be_valid
      end
    end

    context "map" do
      let(:schema) do
        Avro::Builder.build_schema do
          record :has_map do
            required :m, :map, values: :int
          end
        end
      end
      let(:test_class) { described_class.model(schema: schema) }

      it "validates that a required map is not nil" do
        instance = test_class.new(m: nil)
        expect(instance).to be_invalid
        expect(instance.errors[:m]).to include("can't be nil")
      end

      it "allows a required map to be empty" do
        instance = test_class.new(m: {})
        expect(instance).to be_valid
      end
    end
  end

  context "optional" do
    let(:schema_name) { 'test.with_union' }

    it "does not require optional fields to be present" do
      expect(test_class.new).to be_valid
    end
  end
end

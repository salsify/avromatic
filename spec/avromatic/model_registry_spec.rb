describe Avromatic::ModelRegistry do
  let(:model) { Avromatic::Model.model(schema_name: 'test.nested_record') }
  let(:instance) { described_class.new }

  describe "#registered?" do
    context "for a model that has not been registered" do
      it "returns false" do
        expect(instance.registered?('test.nested_record')).to eql(false)
      end
    end

    context "for model that has been registered" do
      before { instance.register(model) }

      it "returns true" do
        expect(instance.registered?('test.nested_record')).to eql(true)
      end
    end
  end

  describe "#register_if_missing" do
    context "for a model that has not been registered" do
      it "registers the model" do
        instance.register_if_missing(model)
        expect(instance.registered?('test.nested_record')).to eql(true)
      end
    end

    context "for model that has been registered" do
      let(:model_copy) { model.dup }
      before { instance.register(model) }

      it "does not raise an error" do
        expect { instance.register_if_missing(model_copy) }.not_to raise_error
      end

      it "does not replace the registered version of the model" do
        instance.register_if_missing(model_copy)
        expect(instance['test.nested_record']).to equal(model)
      end
    end
  end

  context "without a namespace prefix to remove" do
    it "stores a model by its fullname" do
      instance.register(model)
      expect(instance['test.nested_record']).to equal(model)
    end

    context "with a previously registered model" do
      before { instance.register(model) }

      it "raises an error" do
        expect do
          instance.register(model)
        end.to raise_error("'test.nested_record' has already been registered")
      end
    end

    context "for a model with a key schema" do
      let(:model) do
        Avromatic::Model.model(key_schema_name: 'test.encode_key',
                               schema_name: 'test.encode_value')
      end

      it "raises an error" do
        expect { instance.register(model) }
          .to raise_error('models with a key schema are not supported')
      end
    end

    context "when a model has not been registered" do
      it "raises an error" do
        expect { instance['test.fnord'] }
          .to raise_error(/key not found: "test.fnord"/)
      end
    end
  end

  context "with a namespace prefix to remove" do
    let(:multilevel_model) do
      schema = Avro::Builder.build_schema do
        record :rec, namespace: 'test.sub' do
          required :i, :int
        end
      end
      Avromatic::Model.model(schema: schema)
    end

    context "with a String prefix" do
      let(:instance) { described_class.new(remove_namespace_prefix: 'test') }

      it "stores a model by its fullname with prefix removed" do
        instance.register(model)
        expect(instance['nested_record']).to equal(model)
      end

      it "only removes the matching namespace prefix" do
        instance.register(multilevel_model)
        expect(instance['sub.rec']).to equal(multilevel_model)
      end

      context "for a model that does not match the prefix" do
        let(:instance) { described_class.new(remove_namespace_prefix: 'test.sub') }

        it "stores the model by its fullname" do
          instance.register(model)
          expect(instance['test.nested_record']).to equal(model)
        end
      end
    end

    context "with a Regexp prefix" do
      let(:instance) { described_class.new(remove_namespace_prefix: /([\w]+\.)+/) }

      it "stores a model by its fullname with prefix removed" do
        instance.register(model)
        expect(instance['nested_record']).to equal(model)
      end

      it "only removes the matching namespace prefix" do
        instance.register(multilevel_model)
        expect(instance['rec']).to equal(multilevel_model)
      end
    end

    context "with an invalid prefix value" do
      let(:instance) { described_class.new(remove_namespace_prefix: 1) }

      it "raises an error" do
        expect { instance.register(model) }
          .to raise_error('unsupported `remove_namespace_prefix` value: 1')
      end
    end
  end
end

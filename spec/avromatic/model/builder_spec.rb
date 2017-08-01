describe Avromatic::Model::Builder do
  let(:schema_store) { Avromatic.schema_store }
  let(:schema) { schema_store.find(schema_name) }
  let(:key_schema) { schema_store.find(key_schema_name) }
  let(:test_class) do
    described_class.model(schema_name: schema_name)
  end
  let(:mutable_test_class) do
    described_class.model(schema_name: schema_name, mutable: true)
  end
  let(:values) { { s: 'foo', tf: true, i: 42 } }

  let(:attribute_names) do
    test_class.attribute_set.map(&:name).map(&:to_s)
  end

  describe ".model" do
    let(:schema_name) { 'test.primitive_types' }
    let(:klass) do
      described_class.model(schema_name: schema_name)
    end

    it "returns a new model class" do
      expect(klass).to be_a(Class)
      expect(klass.ancestors).to include(Avromatic::Model::Attributes)
      expect(klass.attribute_set.to_a.map(&:name).map(&:to_s))
        .to match_array(schema.fields.map(&:name))
    end

    it "has a name" do
      expect(klass.name).to eq('PrimitiveType')
    end
  end

  context "model generation" do
    context "when a schema is not specified" do
      it "raises an error" do
        expect do
          described_class.new
        end.to raise_error(ArgumentError,
                           'value_schema(_name) or schema(_name) must be specified')
      end
    end

    context "when both a schema and a value_schema are specified" do
      let(:schema_name) { 'test.primitive_types' }

      it "raises an error" do
        expect do
          described_class.new(value_schema: schema, schema: schema)
        end.to raise_error(ArgumentError,
                           'Only one of value_schema(_name) and schema(_name) can be specified')
      end
    end

    context "when both a schema_name and a value schema_name are specified" do
      let(:schema_name) { 'test.primitive_types' }

      it "raises an error" do
        expect do
          described_class.new(value_schema_name: schema_name, schema_name: schema_name)
        end.to raise_error(ArgumentError,
                           'Only one of value_schema(_name) and schema(_name) can be specified')
      end
    end

    shared_examples_for "a generated model" do
      it "defines a model with the expected attributes" do
        expect(attribute_names)
          .to match_array(schema.fields.map(&:name))
      end
    end

    context "primitive types" do
      let(:schema_name) { 'test.primitive_types' }

      it_behaves_like "a generated model"
    end

    context "with a schema" do
      let(:schema_name) { 'test.primitive_types' }

      let(:test_class) do
        Avromatic::Model.model(schema: schema)
      end

      it_behaves_like "a generated model"
    end

    context "named fields" do
      let(:schema_name) { 'test.named_fields' }

      it_behaves_like "a generated model"
    end

    context "with repeated references to a named type" do
      let(:schema_name) { 'test.repeated_name' }

      it_behaves_like "a generated model"
    end

    context "with an array" do
      let(:schema_name) { 'test.with_array' }

      it_behaves_like "a generated model"
    end

    context "with an optional array" do
      let(:schema) do
        Avro::Builder.build_schema do
          record :with_optional_array do
            optional :maybe_array, array(:string)
          end
        end
      end
      let(:test_class) { Avromatic::Model.model(schema: schema) }

      it_behaves_like "a generated model"
    end

    context "with an optional map" do
      let(:schema) do
        Avro::Builder.build_schema do
          record :with_optional_map do
            optional :maybe_map, map(:int)
          end
        end
      end
      let(:test_class) { Avromatic::Model.model(schema: schema) }

      it_behaves_like "a generated model"
    end

    context "with a map" do
      let(:schema_name) { 'test.with_map' }

      it_behaves_like "a generated model"
    end

    context "with a union" do
      let(:schema_name) { 'test.with_union' }

      it_behaves_like "a generated model"
    end

    context "simple union" do
      let(:schema) do
        Avro::Builder.build_schema do
          record :with_simple_union do
            required :u, :union, types: [:int, :string]
          end
        end
      end
      let(:test_class) do
        Avromatic::Model.model(schema: schema)
      end

      it_behaves_like "a generated model"
    end

    context "unsupported union" do
      let(:schema_name) { 'test.real_union' }

      it_behaves_like "a generated model"
    end

    context "top-level union" do
      let(:schema) do
        [
          {
            type: :record,
            name: :foo,
            fields: [{ name: :foo_message, type: :string }]
          },
          {
            type: :record,
            name: :boo,
            fields: [{ name: :bar_message, type: :string }]
          }
        ].to_json
      end
      let(:test_class) do
        Avromatic::Model.model(schema: Avro::Schema.parse(schema))
      end

      it "raises an error" do
        expect { test_class }
          .to raise_error("Unsupported schema type 'union', only 'record' schemas are supported.")
      end
    end

    context "with a key and value" do
      let(:schema_name) { 'test.value' }
      let(:key_schema_name) { 'test.key' }
      let(:test_class) do
        Avromatic::Model.model(value_schema_name: schema_name,
                               key_schema_name: key_schema_name)
      end

      it "defines a model with attributes for the key and value" do
        expect(attribute_names)
          .to match_array(schema.fields.map(&:name) | key_schema.fields.map(&:name))
      end

      context "when the key and value have overlapping fields" do
        let(:key_schema_name) { 'test.key_overlap' }

        it "defines a model with attributes for the key and value" do
          expect(attribute_names)
            .to match_array(schema.fields.map(&:name) | key_schema.fields.map(&:name))
        end
      end

      context "when the key and value have conflicting fields" do
        let(:key_schema_name) { 'test.key_conflict' }

        it "raises an error" do
          expect do
            test_class
          end.to raise_error(/Field 'id' has a different type in each schema:/)
        end
      end
    end

    context "with a key containing an optional field" do
      let(:key_schema_name) { 'test.key_with_optional' }
      let(:schema_name) { 'test.value' }
      let(:test_class) do
        Avromatic::Model.model(value_schema_name: schema_name,
                               key_schema_name: key_schema_name)
      end

      context "when allow_optional_key_fields is false (default)" do
        it "raises an error" do
          expect { test_class }.to raise_error("Optional field 'name' not allowed in key schema.")
        end
      end

      context "when allow_optional_key_fields is true" do
        let(:test_class) do
          Avromatic::Model.model(value_schema_name: schema_name,
                                 key_schema_name: key_schema_name,
                                 allow_optional_key_fields: true)
        end

        it "defines a model with attributes for the key and value" do
          expect(attribute_names)
            .to match_array(schema.fields.map(&:name) | key_schema.fields.map(&:name))
        end
      end
    end

    context "logical types" do
      let(:schema_name) { 'test.logical_types' }

      it_behaves_like "a generated model"
    end

    context "recursive models" do
      let(:schema_name) { 'test.recursive' }

      it_behaves_like "a generated model"
    end
  end

  context "coercion" do
    # This is important for the eventual encoding of a model to Avro

    context "enum" do
      let(:schema_name) { 'test.named_fields' }

      it "coerces the value to a string" do
        instance = test_class.new(e: :C)
        expect(instance.e).to eq('C')
      end
    end
  end

  context "unions" do
    let(:schema) do
      Avro::Builder.build_schema do
        record :with_simple_union do
          required :u, :union, types: [:int, :string]
        end
      end
    end
    let(:test_class) do
      Avromatic::Model.model(schema: schema)
    end

    it "stores values in the member types" do
      expect(test_class.new(u: 1).u).to eq(1)
      expect(test_class.new(u: 'foo').u).to eq('foo')
    end

    context "string member first" do
      let(:schema) do
        Avro::Builder.build_schema do
          record :with_simple_union do
            required :u, :union, types: [:string, :int]
          end
        end
      end

      it "does not coerce if a value matches a member type " do
        expect(test_class.new(u: 1).u).to eq(1)
        expect(test_class.new(u: 'foo').u).to eq('foo')
      end
    end

    context "array of unions" do
      let(:schema) do
        Avro::Builder.build_schema do
          record :with_union_array do
            required :ua, :array, items: union(:string, :int)
          end
        end
      end

      it "coerces stores union values in the array" do
        expect(test_class.new(ua: ['foo', 2]).ua).to eq(['foo', 2])
      end
    end

    context "array of unions with null" do
      let(:schema) do
        Avro::Builder.build_schema do
          record :with_array_of_optional do
            required :aoo, :array, items: union(:null, :string)
          end
        end
      end

      it "coerces values in the array" do
        instance = test_class.new(aoo: [1, nil, 3])
        expect(instance.aoo).to eq(['1', nil, '3'])
      end
    end

    context "union of records" do
      let(:schema_name) { 'test.real_union' }
      let(:test_class) do
        Avromatic::Model.model(schema_name: schema_name)
      end

      let(:value1) do
        { header: 'A', message: { foo_message: 'foo' } }
      end
      let(:value2) do
        { header: 'B', message: { bar_message: 'bar' } }
      end

      it "coerces record members" do
        expect(test_class.new(value1).message.foo_message).to eq('foo')
        expect(test_class.new(value2).message.bar_message).to eq('bar')
      end
    end

    context "array of union of records with a custom type" do
      let(:schema) do
        Avro::Builder.build_schema do
          record :rec_a do
            required :str, :string
          end

          record :rec_b do
            required :i, :int
            required :c, :fixed, size: 6, type_name: :handshake
          end

          record :its_complicated do
            required :top, :record do
              required :a, :array, items: union(:rec_a, :rec_b)
            end
          end
        end
      end
      let(:data) do
        {
          top: { a: [{ str: 137 }, { i: '99', c: 'FooBar' }] }
        }
      end

      before do
        Avromatic.register_type('handshake', String) do |type|
          type.from_avro = ->(value) { value.downcase }
        end
      end

      it "performs the expected coercions" do
        ary = test_class.new(data).top.a
        aggregate_failures do
          expect(ary.first.str).to eq('137')
          expect(ary.last.i).to eq(99)
          expect(ary.last.c).to eq('foobar')
        end
      end
    end

    context "unsupported" do
      context "union with a custom type" do
        let(:schema) do
          Avro::Builder.build_schema do
            fixed :handshake, size: 6
            record :union_with_custom do
              required :u, :union, types: [:string, :handshake]
            end
          end
        end

        before do
          Avromatic.register_type('handshake', String) do |type|
            type.from_avro = ->(value) { value.downcase }
          end
        end

        it "raises an error" do
          expect { test_class }
            .to raise_error('custom types within unions are currently unsupported')
        end
      end

      context "null after the first union member type" do
        let(:schema_name) { 'test.null_in_union' }
        let(:test_class) do
          described_class.model(schema_name: schema_name)
        end

        it "raises an error" do
          expect { test_class }
            .to raise_error('a null type in a union must be the first member')
        end
      end
    end
  end

  context "defaults" do
    let(:schema_name) { 'test.defaults' }
    let(:instance) { test_class.new }

    context "enum" do
      it "returns the default for an enum" do
        expect(instance.defaulted_enum).to eq('A')
      end

      it "freezes the default" do
        expect(instance.defaulted_enum).to be_frozen
      end

      it "includes the default in the attributes hash" do
        expect(instance.attributes[:defaulted_enum]).to eq('A')
      end
    end

    context "other types" do
      context "string" do
        it "returns the default" do
          expect(instance.defaulted_string).to eq('fnord')
        end

        it "freezes the default" do
          expect(instance.defaulted_string).to be_frozen
        end

        it "includes the default in the attributes hash" do
          expect(instance.attributes[:defaulted_string]).to eq('fnord')
        end
      end

      context "int" do
        it "returns the default" do
          expect(instance.defaulted_int).to eq(42)
        end

        it "includes the default in the attributes hash" do
          expect(instance.attributes[:defaulted_int]).to eq(42)
        end
      end
    end
  end

  context "mutable models" do
    let(:schema_name) { 'test.primitive_types' }
    let(:mutable_model) { mutable_test_class.new({s: 'old value'}) }

    it "allows changes to models" do
      expect do
        mutable_model.s = 'new value'
      end.not_to raise_error
    end
  end

  context "value objects" do
    let(:schema_name) { 'test.primitive_types' }
    let(:model1) { test_class.new(values) }
    let(:model2) { test_class.new(values) }
    let(:model3) { test_class.new(values.merge(s: 'bar')) }
    let(:subclass) { Class.new(test_class) }
    let(:submodel) { subclass.new(values) }

    context "immutability" do
      it "prevents changes to models" do
        expect do
          model1.s = 'new value'
        end.to raise_error(NoMethodError, /private method `s=' called for/)
      end
    end

    describe "#eql?" do
      it "compares models with the same attributes as equal" do
        expect(model1).to eql(model2)
      end

      it "compares models with the different attributes as not equal" do
        expect(model1).not_to eql(model3)
      end

      it "compares subclass models with the same attributes as different" do
        expect(model1).not_to eql(submodel)
      end
    end

    describe "#==" do
      it "compares models with the same attributes as equivalent" do
        expect(model1).to eq(model2)
      end

      it "compares models with different attributes as not equivalent" do
        expect(model1).not_to eq(model3)
      end

      it "compares subclass models with the same attributes as different" do
        expect(model1).not_to eq(submodel)
      end
    end

    describe "#hash" do
      it "generates the same hash for models with the same attributes" do
        expect(model1.hash).to eq(model2.hash)
      end

      it "generates a different hash for models with different attributes" do
        expect(model1.hash).not_to eq(model3.hash)
      end

      it "generates the same hash for a subclass model with the same attributes" do
        expect(model1.hash).to eq(submodel.hash)
      end
    end

    describe "#clone" do
      it "returns the same model" do
        expect(model1.clone).to equal(model1)
      end
    end

    describe "#dup" do
      it "returns the same model" do
        expect(model1.dup).to equal(model1)
      end
    end

    describe "#inspect" do
      it "returns the class name and instance attributes" do
        expect(model1.inspect)
          .to eq('#<PrimitiveType s: "foo", b: nil, tf: true, i: 42, l: nil, f: nil, d: nil, n: nil>')
      end
    end

    describe "#to_s" do
      it "returns the class name and encoded object id" do
        expect(model1.to_s).to match(/#<PrimitiveType:.*>/)
      end
    end
  end
end

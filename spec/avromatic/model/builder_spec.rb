# frozen_string_literal: true

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
    test_class.attribute_definitions.keys.map(&:to_s)
  end

  describe ".model" do
    let(:schema_name) { 'test.primitive_types' }
    let(:klass) do
      described_class.model(schema_name: schema_name)
    end

    it "returns a new model class" do
      expect(klass).to be_a(Class)
      expect(klass.ancestors).to include(Avromatic::Model::Attributes)
      expect(klass.attribute_definitions.keys.map(&:to_s))
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

    context "when there are incompatible embedded schemas" do
      let(:outer_schema) do
        Avro::Builder.build_schema do
          record :inner_record do
            required :a, :string
          end

          record :outer_record do
            required :inner, :inner_record
          end
        end
      end

      let(:incompatible_inner_schema) do
        Avro::Builder.build_schema do
          record :inner_record do
            required :a, :int
          end
        end
      end

      before do
        described_class.model(schema: incompatible_inner_schema)
      end

      it "raises an error during model definition" do
        expect do
          described_class.model(schema: outer_schema)
        end.to raise_error('The InnerRecord model is already registered with an incompatible version of the inner_record schema')
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

      it "defines a boolean accessor that returns false for a false value" do
        instance = test_class.new(tf: false)
        expect(instance.tf?).to eq(false)
      end

      it "defines a boolean accessor that returns true for a true value" do
        instance = test_class.new(tf: true)
        expect(instance.tf?).to eq(true)
      end

      it "defines a boolean accessor that returns false for a null value" do
        instance = test_class.new(tf: nil)
        expect(instance.tf?).to eq(false)
      end

      context "with an optional boolean" do
        let(:schema_name) { 'test.optional_boolean' }

        it "defines a boolean accessor" do
          instance = test_class.new(b: true)
          expect(instance.b?).to eq(true)
        end
      end
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

      context "timestamp-millis" do
        it "coerces a Time" do
          time = Time.now
          instance = test_class.new(ts_msec: time)
          expect(instance.ts_msec).to eq(::Time.at(time.to_i, time.usec / 1000 * 1000))
        end

        it "coerces a DateTime" do
          time = DateTime.now # rubocop:disable Style/DateTime
          instance = test_class.new(ts_msec: time)
          expect(instance.ts_msec).to eq(::Time.at(time.to_i, time.usec / 1000 * 1000))
        end

        it "coerces an ActiveSupport::TimeWithZone" do
          Time.zone = 'GMT'
          time = Time.zone.now
          instance = test_class.new(ts_msec: time)
          expect(instance.ts_msec).to eq(::Time.at(time.to_i, time.usec / 1000 * 1000))
        end

        it "raises an Avromatic::Model::CoercionError when the value is a Date" do
          expect { test_class.new(ts_msec: Date.today) }.to raise_error(Avromatic::Model::CoercionError)
        end
      end

      context "timestamp-micros" do
        it "coerces a Time" do
          time = Time.now
          instance = test_class.new(ts_usec: time)
          expect(instance.ts_usec).to eq(::Time.at(time.to_i, time.usec))
        end

        it "coerces a DateTime" do
          time = DateTime.now # rubocop:disable Style/DateTime
          instance = test_class.new(ts_usec: time)
          expect(instance.ts_usec).to eq(::Time.at(time.to_i, time.usec))
        end

        it "coerces an ActiveSupport::TimeWithZone" do
          Time.zone = 'GMT'
          time = Time.zone.now
          instance = test_class.new(ts_usec: time)
          expect(instance.ts_usec).to eq(::Time.at(time.to_i, time.usec))
        end

        it "raises an Avromatic::Model::CoercionError when the value is a Date" do
          expect { test_class.new(ts_usec: Date.today) }.to raise_error(Avromatic::Model::CoercionError)
        end
      end

      context "date" do
        it "accepts a Date" do
          today = Date.today
          instance = test_class.new(date: today)
          expect(instance.date).to eq(today)
        end

        it "accepts a Time" do
          time = Time.now
          instance = test_class.new(date: time)
          expect(instance.date).to eq(::Date.new(time.year, time.month, time.day))
        end

        it "accepts a DateTime" do
          time = DateTime.now # rubocop:disable Style/DateTime
          instance = test_class.new(date: time)
          expect(instance.date).to eq(::Date.new(time.year, time.month, time.day))
        end

        it "raises an Avromatic::Model::CoercionError when the value is not coercible" do
          expect { test_class.new(date: 'today') }.to raise_error(Avromatic::Model::CoercionError)
        end
      end
    end

    context "recursive models" do
      let(:schema_name) { 'test.recursive' }

      it_behaves_like "a generated model"
    end
  end

  shared_examples_for "a reader of attribute values" do |method_name|
    let(:schema_name) { 'test.primitive_types' }
    let(:attributes) do
      {
        s: 'foo',
        b: '123',
        tf: true,
        i: 777,
        l: 123456789,
        f: 0.5,
        d: 1.0 / 3.0,
        n: nil,
        fx: '1234567',
        e: 'A'
      }
    end
    let(:instance) { test_class.new(attributes) }

    it "returns the correct attributes" do
      expect(instance.send(method_name)).to eq(attributes)
    end

    it "returns a copy of the mutable hash" do
      expect do
        instance.send(method_name)[:s] = 'updated'
      end.not_to change(instance, :s)
    end
  end

  describe "#to_h" do
    it_behaves_like "a reader of attribute values", :to_h
  end

  describe "#to_hash" do
    it_behaves_like "a reader of attribute values", :to_hash
  end

  describe "#attributes" do
    it_behaves_like "a reader of attribute values", :attributes
  end

  describe "#initialize" do
    let(:schema_name) { 'test.primitive_types' }

    it "raises an Avromatic::Model::UnknownAttributeError when passed an unknown attribute when allow_unknown_attributes is false" do
      input = { unknown: true }
      expect do
        test_class.new(input)
      end.to raise_error(Avromatic::Model::UnknownAttributeError,
                         'Unexpected arguments for PrimitiveType#initialize: unknown. ' \
                          "Only the following arguments are allowed: #{test_class.attribute_definitions.keys.map(&:to_s).sort.join(', ')}. " \
                          "Provided arguments: #{input.inspect}")
    end

    it "does not raise an Avromatic::Model::UnknownAttributeError when passed an unknown attribute when allow_unknown_attributes is true" do
      allow(Avromatic).to receive(:allow_unknown_attributes).and_return(true)
      expect { test_class.new(unknown: true) }.not_to raise_error
    end

    context "when the model has a super class" do
      let(:parent_class) do
        Class.new do
          attr_reader :parent_initialized
          def initialize
            @parent_initialized = true
          end
        end
      end

      let(:test_class) do
        Class.new(parent_class) do
          include Avromatic::Model.build(schema_name: 'test.primitive_types')
        end
      end

      it "calls the super class' initialize method" do
        instance = test_class.new(s: 's')
        expect(instance.parent_initialized).to eq(true)
      end
    end
  end

  context "coercion" do
    # This is important for the eventual encoding of a model to Avro

    context "primitives" do
      let(:schema_name) { 'test.primitive_types' }

      context "string" do
        it "coerces a string to a string" do
          instance = test_class.new(s: 'foo')
          expect(instance.s).to eq('foo')
        end

        it "coerces a symbol to a string" do
          instance = test_class.new(s: :foo)
          expect(instance.s).to eq('foo')
        end

        it "coerces a nil to a string" do
          instance = test_class.new(s: nil)
          expect(instance.s).to be_nil
        end

        it "does not coerce an integer to a string" do
          expect { test_class.new(s: 100) }.to raise_error(Avromatic::Model::CoercionError)
        end
      end

      context "fixed" do
        it "coerces a string of the appropriate length to a fixed" do
          instance = test_class.new(fx: '1234567')
          expect(instance.fx).to eq('1234567')
        end

        it "coerces a nil to a fixed" do
          instance = test_class.new(fx: nil)
          expect(instance.fx).to be_nil
        end

        it "does not coerce an integer to a fixed" do
          expect { test_class.new(fx: 1234567) }.to raise_error(Avromatic::Model::CoercionError)
        end

        it "does not coerce a string that is too short to a fixed" do
          expect { test_class.new(fx: '123456') }.to raise_error(Avromatic::Model::CoercionError)
        end

        it "does not coerce a string that is too long to a fixed" do
          expect { test_class.new(fx: '12345678') }.to raise_error(Avromatic::Model::CoercionError)
        end
      end

      context "integer" do
        it "coerces a integer to a integer" do
          instance = test_class.new(i: 1)
          expect(instance.i).to eq(1)
        end

        it "coerces a nil to an integer" do
          instance = test_class.new(i: nil)
          expect(instance.i).to be_nil
        end

        it "does not coerce a string to an integer" do
          expect { test_class.new(i: '100') }.to raise_error(Avromatic::Model::CoercionError)
        end
      end

      context "boolean" do
        it "coerces a false to a boolean" do
          instance = test_class.new(tf: false)
          expect(instance.tf).to eq(false)
        end

        it "coerces a true to a boolean" do
          instance = test_class.new(tf: true)
          expect(instance.tf).to eq(true)
        end

        it "coerces a nil to a boolean" do
          instance = test_class.new(tf: nil)
          expect(instance.tf).to be_nil
        end

        it "does not coerce a string to a boolean" do
          expect { test_class.new(tf: 'true') }.to raise_error(Avromatic::Model::CoercionError)
        end
      end

      context "bytes" do
        it "coerces a string to bytes" do
          instance = test_class.new(b: 'foo')
          expect(instance.b).to eq('foo')
        end

        it "coerces a nil to bytes" do
          instance = test_class.new(b: nil)
          expect(instance.b).to be_nil
        end

        it "does not coerce a number to bytes" do
          expect { test_class.new(b: 12) }.to raise_error(Avromatic::Model::CoercionError)
        end
      end

      context "long" do
        it "coerces a long to a long" do
          instance = test_class.new(l: 123)
          expect(instance.l).to eq(123)
        end

        it "coerces a nil to a long" do
          instance = test_class.new(l: nil)
          expect(instance.l).to be_nil
        end

        it "does not coerce a string to a long" do
          expect { test_class.new(l: '12') }.to raise_error(Avromatic::Model::CoercionError)
        end
      end

      context "float" do
        it "coerces a long to a float" do
          instance = test_class.new(f: 1.23)
          expect(instance.f).to eq(1.23)
        end

        it "coerces a nil to a float" do
          instance = test_class.new(f: nil)
          expect(instance.f).to be_nil
        end

        it "coerces an integer to a float" do
          instance = test_class.new(f: 123)
          expect(instance.f).to eq(123.0)
        end

        it "does not coerce a string to a float" do
          expect { test_class.new(f: '1.22') }.to raise_error(Avromatic::Model::CoercionError)
        end
      end

      context "double" do
        it "coerces a long to a double" do
          instance = test_class.new(d: 1.23)
          expect(instance.d).to eq(1.23)
        end

        it "coerces a nil to a double" do
          instance = test_class.new(d: nil)
          expect(instance.d).to be_nil
        end

        it "coerces an integer to a double" do
          instance = test_class.new(f: 123)
          expect(instance.f).to eq(123.0)
        end

        it "does not coerce a string to a double" do
          expect { test_class.new(d: '1.22') }.to raise_error(Avromatic::Model::CoercionError)
        end
      end

      context "null" do
        it "coerces a nil to a nil" do
          instance = test_class.new(n: nil)
          expect(instance.n).to be_nil
        end

        it "does not coerce an empty string to nil" do
          expect { test_class.new(n: '') }.to raise_error(Avromatic::Model::CoercionError)
        end
      end

      context "enum" do
        it "coerces a string to an enum" do
          instance = test_class.new(e: 'A')
          expect(instance.e).to eq('A')
        end

        it "coerces a symbol to an enum" do
          instance = test_class.new(e: :A)
          expect(instance.e).to eq('A')
        end

        it "coerces a nil to an enum" do
          instance = test_class.new(e: nil)
          expect(instance.e).to be_nil
        end

        it "does not coerce an unallowed string to an enum" do
          expect { test_class.new(e: 'invalid') }.to raise_error(Avromatic::Model::CoercionError)
        end

        it "does not coerce an unallowed symbol to an enum" do
          expect { test_class.new(e: :invalid) }.to raise_error(Avromatic::Model::CoercionError)
        end

        it "does not coerce an integer to an enum" do
          expect { test_class.new(e: 100) }.to raise_error(Avromatic::Model::CoercionError)
        end
      end

      context "custom type" do
        let(:schema) do
          Avro::Builder.build_schema do
            fixed :handshake, size: 6
            record :record_with_custom_type do
              required :h, :handshake
            end
          end
        end

        let(:test_class) do
          described_class.model(schema: schema)
        end

        before do
          Avromatic.register_type('handshake', String) do |type|
            type.from_avro = ->(value) { value.downcase }
          end
        end

        it "coerces to the custom type when the input is coercible" do
          instance = test_class.new(h: 'VALUE')
          expect(instance.h).to eq('value')
        end

        it "coerces to nil to nil" do
          instance = test_class.new(h: nil)
          expect(instance.h).to be_nil
        end

        it "raises an exception for uncoercible input" do
          expect { test_class.new(h: 1) }.to raise_error(Avromatic::Model::CoercionError)
        end
      end
    end

    context "records" do
      let(:schema_name) { 'test.nested_record' }

      it "coerces a hash to a model" do
        instance = test_class.new(sub: { str: 'a', i: 1 })
        expect(instance.sub).to eq(Avromatic.nested_models['test.__nested_record_sub_record'].new(str: 'a', i: 1))
      end

      it "coerces a nil to a null" do
        instance = test_class.new(sub: nil)
        expect(instance.sub).to be_nil
      end

      it "does not coerce a string" do
        sub_input = 'foobar'
        expect do
          test_class.new(sub: sub_input)
        end.to raise_error(Avromatic::Model::CoercionError,
                           'Value for NestedRecord#sub could not be coerced to a NestedRecordSubRecord ' \
                             'because a String was provided but expected a NestedRecordSubRecord or Hash. ' \
                             "Provided argument: #{sub_input.inspect}")
      end

      it "does not coerce hashes with additional attributes" do
        sub_input = { 'str' => 'a', 'i' => 1, 'b' => 1 }
        expect do
          test_class.new(sub: sub_input)
        end.to raise_error(Avromatic::Model::CoercionError,
                           'Value for NestedRecord#sub could not be coerced to a NestedRecordSubRecord because the ' \
                             'following unexpected attributes were provided: b. Only the following attributes are allowed: i, str. ' \
                             "Provided argument: #{sub_input.inspect}")
      end
    end

    context "arrays" do
      let(:schema) do
        Avro::Builder.build_schema do
          record :record_with_array do
            required :a, array(:string)
          end
        end
      end

      let(:test_class) do
        described_class.model(schema: schema)
      end

      it "coerces elements in the array" do
        instance = test_class.new(a: [:foo])
        expect(instance.a).to eq(['foo'])
      end

      it "coerces a nil to a nil" do
        instance = test_class.new(a: nil)
        expect(instance.a).to be_nil
      end

      it "raises an exception for non-Arrays" do
        expect { test_class.new(a: 'foobar') }.to raise_error(Avromatic::Model::CoercionError)
      end
    end

    context "maps" do
      let(:schema) do
        Avro::Builder.build_schema do
          record :record_with_map do
            required :m, map(:string)
          end
        end
      end

      let(:test_class) do
        described_class.model(schema: schema)
      end

      it "coerces elements in the map" do
        instance = test_class.new(m: { foo: :bar })
        expect(instance.m).to eq('foo' => 'bar')
      end

      it "coerces a nil to a nil" do
        instance = test_class.new(m: nil)
        expect(instance.m).to be_nil
      end

      it "raises an exception for non-Hashes" do
        expect { test_class.new(m: 'foobar') }.to raise_error(Avromatic::Model::CoercionError)
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

    it "coerces a nil to nil" do
      expect(test_class.new(u: nil).u).to be_nil
    end

    it "raises an Avromatic::Model::CoercionError for input that can't be coerced to a member type" do
      expect { test_class.new(u: { foo: 'bar' }) }.to raise_error(Avromatic::Model::CoercionError)
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
        instance = test_class.new(aoo: ['foo', nil, :bar])
        expect(instance.aoo).to eq(['foo', nil, 'bar'])
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

      it "does not coerce hashes with keys that don't match a union member's type" do
        message_input = { foo_message: 'foo', bar_message: 'bar' }
        expect do
          test_class.new(header: 'B', message: message_input)
        end.to raise_error(Avromatic::Model::CoercionError,
                           'Value for RealUnion#message could not be coerced to a union[Foo, Bar] ' \
                              "because no union member type has all of the provided attributes: #{message_input.inspect}")
      end
    end

    context "union of records with overlapping fields" do
      let(:schema) do
        Avro::Builder.build_schema do
          record :sub1 do
            required :a, :string
          end

          record :sub2 do
            required :a, :string
            required :b, :int
          end

          record :sub3 do
            required :a, :string
            required :b, :string
          end

          record :sub4 do
            required :a, :string
            required :b, :string
            optional :c, :string
          end

          record :with_union do
            required :u, :union, types: [:sub1, :sub2, :sub3, :sub4]
          end
        end
      end

      it "coerces to the first union member with all of the specified attribute values with the correct types" do
        instance = test_class.new(u: { a: :foo, b: :bar })
        expect(instance.u.a).to eq('foo')
        expect(instance.u.b).to eq('bar')
        expect(instance.u).to be_an_instance_of(Avromatic.nested_models['sub3'])
      end
    end

    context "union of arrays" do
      let(:schema) do
        Avro::Builder.build_schema do
          record :with_array_union do
            required :u, :union, types: [:string, array(:string)]
          end
        end
      end

      it "coerces a string array to a union member" do
        instance = test_class.new(u: %(a b))
        expect(instance.u).to eq(%(a b))
      end
    end

    context "union of maps" do
      let(:schema) do
        Avro::Builder.build_schema do
          record :with_map_union do
            required :u, :union, types: [:string, map(:string)]
          end
        end
      end

      it "coerces a map to a union member" do
        instance = test_class.new(u: { a: 'b' })
        expect(instance.u).to eq('a' => 'b')
      end
    end

    context "union with a custom type" do
      let(:schema) do
        Avro::Builder.build_schema do
          fixed :handshake, size: 6
          record :union_with_custom do
            required :u, :union, types: [:long, :boolean, :double, :handshake]
          end
        end
      end

      before do
        Avromatic.register_type('handshake', String) do |type|
          type.from_avro = ->(value) { value.downcase }
        end
      end

      it "performs the expected coercion" do
        instance = test_class.new(u: 'VALUE')
        expect(instance.u).to eq('value')
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
          top: { a: [{ str: '137' }, { i: 99, c: 'FooBar' }] }
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

    context "union with logical types" do
      let(:test_class) do
        Avromatic::Model.model(schema: schema)
      end

      context "union with a timestamp-micros" do
        let(:schema) do
          Avro::Builder.build_schema do
            record :with_date_union do
              required :u, :union, types: [:string, long(logical_type: 'timestamp-micros')]
            end
          end
        end

        it "coerces a time to a union member" do
          now = Time.now
          instance = test_class.new(u: now)
          expect(instance.u).to eq(Time.at(now.to_i, now.usec))
        end
      end

      context "union with a timestamp-millis" do
        let(:schema) do
          Avro::Builder.build_schema do
            record :with_date_union do
              required :u, :union, types: [:string, long(logical_type: 'timestamp-millis')]
            end
          end
        end

        it "coerces a time to a union member" do
          now = Time.now
          instance = test_class.new(u: now)
          expect(instance.u).to eq(Time.at(now.to_i, now.usec / 1000 * 1000))
        end
      end

      context "union with a date" do
        let(:schema) do
          Avro::Builder.build_schema do
            record :with_date_union do
              required :u, :union, types: [:string, long(logical_type: 'date')]
            end
          end
        end

        it "coerces dates to a union member" do
          now = Date.today
          instance = test_class.new(u: now)
          expect(instance.u).to eq(now)
        end
      end
    end

    context "unsupported" do
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

    it "does not override attributes that have already been set" do
      test_class.send(:define_method, :initialize) do |attributes = {}|
        self.defaulted_string = 'other_value'
        super(attributes)
      end
      instance = test_class.new
      expect(instance.defaulted_string).to eq('other_value')
    end
  end

  context "mutable models" do
    let(:schema_name) { 'test.primitive_types' }
    let(:mutable_model) { mutable_test_class.new(s: 'old value') }

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
          .to eq('#<PrimitiveType s: "foo", b: nil, tf: true, i: 42, l: nil, f: nil, d: nil, n: nil, fx: nil, e: nil>')
      end
    end

    describe "#to_s" do
      it "returns the class name and encoded object id" do
        expect(model1.to_s).to match(/#<PrimitiveType:.*>/)
      end
    end
  end
end

describe Avromatic::Model::Builder, 'validation' do
  let(:schema) { schema_store.find(schema_name) }
  let(:test_class) do
    described_class.model(schema_name: schema_name)
  end
  let(:attribute_names) do
    test_class.attribute_definitions.keys.map(&:to_s)
  end

  context "primitives" do
    let(:schema_name) { 'test.primitive_types' }

    context "string" do
      it "validates that a string has the correct type" do
        instance = test_class.new(s: { x: 1 })
        expect(instance).to be_invalid
        expect(instance.errors[:s]).to include('does not have the expected type [String]')
      end
    end

    context "integer" do
      it "validates that an integer has the correct type" do
        instance = test_class.new(i: { x: 2 })
        expect(instance).to be_invalid
        expect(instance.errors[:i]).to include('does not have the expected type [Integer]')
      end
    end

    context "boolean" do
      it "validates that a boolean has the correct type" do
        instance = test_class.new(tf: { x: 3 })
        expect(instance).to be_invalid
        expect(instance.errors[:tf]).to include('does not have the expected type [TrueClass, FalseClass]')
      end
    end

    context "bytes" do
      it "validates that bytes have the correct type" do
        instance = test_class.new(b: { x: 4 })
        expect(instance).to be_invalid
        expect(instance.errors[:b]).to include('does not have the expected type [String]')
      end
    end

    context "long" do
      it "validates that a long has the correct type" do
        instance = test_class.new(l: { x: 5 })
        expect(instance).to be_invalid
        expect(instance.errors[:l]).to include('does not have the expected type [Integer]')
      end
    end

    context "float" do
      it "validates that a float has the correct type" do
        instance = test_class.new(f: { x: 6 })
        expect(instance).to be_invalid
        expect(instance.errors[:f]).to include('does not have the expected type [Float]')
      end
    end

    context "double" do
      it "validates that a double has the correct type" do
        instance = test_class.new(d: { x: 7 })
        expect(instance).to be_invalid
        expect(instance.errors[:d]).to include('does not have the expected type [Float]')
      end
    end

    context "null" do
      it "validates that a null field has the correct type" do
        instance = test_class.new(n: { x: 8 })
        expect(instance).to be_invalid
        expect(instance.errors[:n]).to include('does not have the expected type [NilClass]')
      end
    end
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

  context "logical types" do
    let(:schema_name) { 'test.logical_types' }

    context "timestamp-millis" do
      it "accepts a Time" do
        instance = test_class.new(ts_msec: Time.now)
        instance.validate
        expect(instance.errors[:ts_msec]).to be_empty
      end

      it "accepts an ActiveSupport::TimeWithZone" do
        Time.zone = 'GMT'
        instance = test_class.new(ts_msec: Time.zone.now)
        instance.validate
        expect(instance.errors[:ts_msec]).to be_empty
      end

      it "validates that a timestamp-millis is a Time" do
        instance = test_class.new(ts_msec: Date.today)
        expect(instance).to be_invalid
        expect(instance.errors[:ts_msec]).to include('does not have the expected type [Time]')
      end
    end

    context "timestamp-micros" do
      it "accepts a Time" do
        instance = test_class.new(ts_usec: Time.now)
        instance.validate
        expect(instance.errors[:ts_usec]).to be_empty
      end

      it "accepts an ActiveSupport::TimeWithZone" do
        Time.zone = 'GMT'
        instance = test_class.new(ts_usec: Time.zone.now)
        instance.validate
        expect(instance.errors[:ts_usec]).to be_empty
      end

      it "validates that a timestamp-micros is a Time" do
        instance = test_class.new(ts_usec: Date.today)
        expect(instance).to be_invalid
        expect(instance.errors[:ts_usec]).to include('does not have the expected type [Time]')
      end
    end

    context "date" do
      it "accepts a Date" do
        instance = test_class.new(date: Date.today)
        instance.validate
        expect(instance.errors[:date]).to be_empty
      end

      it "accepts a Time" do
        instance = test_class.new(date: Time.now)
        instance.validate
        expect(instance.errors[:date]).to be_empty
      end

      it "validates that a date is a Date" do
        Time.zone = 'GMT'
        instance = test_class.new(date: Time.zone.now)
        expect(instance).to be_invalid
        expect(instance.errors[:date]).to include('does not have the expected type [Date]')
      end
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
        pending "Virtus coerces nil values to an empty array"
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

    context "nested records" do
      let(:schema) do
        Avro::Builder.build_schema do
          record :has_record do
            required :sub, :record, type_name: 'sub_type' do
              required :i, :int
              required :s, :string
            end
          end
        end
      end
      let(:test_class) { described_class.model(schema: schema) }

      it "validates nested records" do
        instance = test_class.new(sub: test_class.nested_models['sub_type'].new)
        expect(instance).to be_invalid
        aggregate_failures do
          expect(instance.errors[:sub]).to include(".i can't be blank")
          expect(instance.errors[:sub]).to include(".s can't be blank")
        end
      end

      it "validates nested records initialized with a hash" do
        instance = test_class.new(sub: { i: 0 })
        expect(instance).to be_invalid
        expect(instance.errors[:sub]).to include(".s can't be blank")
      end

      context "doubly nested record" do
        let(:schema) do
          Avro::Builder.build_schema do
            record :outer do
              required :sub, :record, type_name: 'level1' do
                required :sub_sub, :record, type_name: 'level2' do
                  required :l, :long
                end
              end
            end
          end
        end

        it "validates multiple levels of nesting" do
          level1 = test_class.nested_models['level1']
          level2 = test_class.nested_models['level2']
          instance = test_class.new(sub: level1.new(sub_sub: level2.new))
          expect(instance).to be_invalid
          expect(instance.errors[:sub]).to include(".sub_sub.l can't be blank")
        end

        it "validates multiple levels of nesting initialized with a hash" do
          instance = test_class.new(sub: { sub_sub: {} })
          expect(instance).to be_invalid
          expect(instance.errors[:sub]).to include(".sub_sub.l can't be blank")
        end
      end

      context "array of records" do
        let(:schema) do
          Avro::Builder.build_schema do
            record :x_and_y do
              required :x, :int
              required :y, :int
            end

            record :array_of_records do
              required :ary, :array, items: :x_and_y
            end
          end
        end

        it "validates records in an array" do
          nested_model = test_class.nested_models['x_and_y']
          data = [nested_model.new,
                  nested_model.new(x: 1),
                  nested_model.new(y: 2)]
          instance = test_class.new(ary: data)
          expect(instance).to be_invalid
          aggregate_failures do
            expect(instance.errors[:ary]).to include("[0].x can't be blank")
            expect(instance.errors[:ary]).to include("[0].y can't be blank")
            expect(instance.errors[:ary]).to include("[1].y can't be blank")
            expect(instance.errors[:ary]).to include("[2].x can't be blank")
          end
        end

        it "validates records in an array initialized with hashes" do
          data = [{},
                  { x: 1 },
                  { y: 2 }]
          instance = test_class.new(ary: data)
          expect(instance).to be_invalid
          aggregate_failures do
            expect(instance.errors[:ary]).to include("[0].x can't be blank")
            expect(instance.errors[:ary]).to include("[0].y can't be blank")
            expect(instance.errors[:ary]).to include("[1].y can't be blank")
            expect(instance.errors[:ary]).to include("[2].x can't be blank")
          end
        end
      end

      context "nested arrays of records" do
        let(:schema) do
          Avro::Builder.build_schema do
            record :elt do
              required :s, :string
            end

            record :with_matrix do
              required :m, array(array(:elt))
            end
          end
        end
        let(:nested_model) { test_class.nested_models['elt'] }

        it "validates deeply nested records" do
          data = [
            [nested_model.new(s: 'a'), nested_model.new],
            [nested_model.new(s: 'c'), nested_model.new, nested_model.new(s: 'b')]
          ]
          instance = test_class.new(m: data)
          expect(instance).to be_invalid
          aggregate_failures do
            expect(instance.errors[:m]).to include("[0][1].s can't be blank")
            expect(instance.errors[:m]).to include("[1][1].s can't be blank")
          end
        end
      end

      context "map of records" do
        let(:schema) do
          Avro::Builder.build_schema do
            record :x_and_y do
              required :x, :int
              required :y, :int
            end

            record :array_of_records do
              required :map, :map, values: :x_and_y
            end
          end
        end

        it "validates records in a map" do
          nested_model = test_class.nested_models['x_and_y']
          data = {
            a: nested_model.new(y: 3),
            b: nested_model.new,
            c: nested_model.new(x: 4)
          }
          instance = test_class.new(map: data)
          expect(instance).to be_invalid
          aggregate_failures do
            expect(instance.errors[:map]).to include("['a'].x can't be blank")
            expect(instance.errors[:map]).to include("['b'].y can't be blank")
            expect(instance.errors[:map]).to include("['b'].y can't be blank")
            expect(instance.errors[:map]).to include("['c'].y can't be blank")
          end
        end

        it "validates records in a map initialized with hashes" do
          data = {
            a: { y: 3 },
            b: {},
            c: { x: 4 }
          }
          instance = test_class.new(map: data)
          expect(instance).to be_invalid
          aggregate_failures do
            expect(instance.errors[:map]).to include("['a'].x can't be blank")
            expect(instance.errors[:map]).to include("['b'].y can't be blank")
            expect(instance.errors[:map]).to include("['b'].y can't be blank")
            expect(instance.errors[:map]).to include("['c'].y can't be blank")
          end
        end
      end

      context "record in a union" do
        let(:schema) do
          Avro::Builder.build_schema do
            record :x_and_y do
              required :x, :int
              required :y, :int
            end

            record :with_union do
              required :u, :union, types: [:x_and_y, :string]
            end
          end
        end

        it "validates a record in a union" do
          expect(test_class.new(u: 'foo')).to be_valid
          instance = test_class.new(u: test_class.nested_models['x_and_y'].new)
          expect(instance).to be_invalid
          aggregate_failures do
            expect(instance.errors[:u]).to include(".x can't be blank")
            expect(instance.errors[:u]).to include(".y can't be blank")
          end
        end

        it "validates a record in a union initialized with hashes" do
          expect(test_class.new(u: { x: 1, y: 2 })).to be_valid
          instance = test_class.new(u: {})
          expect(instance).to be_invalid
          expect(instance.errors[:u]).to include("can't be blank")
        end

        it "validates a record in a union initialized with incomplete hashes" do
          instance = test_class.new(u: { x: 1 })
          expect(instance).to be_invalid
          expect(instance.errors[:u]).to include("can't be blank")
        end
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

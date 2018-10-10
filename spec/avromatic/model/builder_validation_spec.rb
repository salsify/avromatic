describe Avromatic::Model::Builder, 'validation' do
  let(:schema) { schema_store.find(schema_name) }
  let(:test_class) do
    described_class.model(schema_name: schema_name)
  end
  let(:attribute_names) do
    test_class.attribute_definitions.keys.map(&:to_s)
  end

  context "required" do
    context "primitive types" do
      let(:schema_name) { 'test.primitive_types' }
      let(:valid_attributes) do
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

      it "validates that required fields must be present" do
        instance = test_class.new
        aggregate_failures do
          expect(instance).to be_invalid
          expect(instance.errors[:base]).to include("s can't be nil")
          expect(instance.errors[:base]).to include("tf can't be nil")
          # All attributes are invalid except the field with type null
          expect(instance.errors.size).to eq(attribute_names.size - 1)
        end
      end

      context "boolean" do
        it "allows a boolean field to be false" do
          instance = test_class.new(valid_attributes.merge(tf: false))
          expect(instance).to be_valid
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
        instance = test_class.new(a: nil)
        expect(instance).to be_invalid
        expect(instance.errors[:base]).to match_array(["a can't be nil"])
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
        expect(instance.errors[:base]).to include("m can't be nil")
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
          expect(instance.errors[:base]).to include("sub.i can't be nil")
          expect(instance.errors[:base]).to include("sub.s can't be nil")
        end
      end

      it "validates nested records initialized with a hash" do
        instance = test_class.new(sub: { i: 0 })
        expect(instance).to be_invalid
        expect(instance.errors[:base]).to include("sub.s can't be nil")
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
          expect(instance.errors[:base]).to include("sub.sub_sub.l can't be nil")
        end

        it "validates multiple levels of nesting initialized with a hash" do
          instance = test_class.new(sub: { sub_sub: {} })
          expect(instance).to be_invalid
          expect(instance.errors[:base]).to include("sub.sub_sub.l can't be nil")
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
            expect(instance.errors[:base]).to include("ary[0].x can't be nil")
            expect(instance.errors[:base]).to include("ary[0].y can't be nil")
            expect(instance.errors[:base]).to include("ary[1].y can't be nil")
            expect(instance.errors[:base]).to include("ary[2].x can't be nil")
          end
        end

        it "validates records in an array initialized with hashes" do
          data = [{},
                  { x: 1 },
                  { y: 2 }]
          instance = test_class.new(ary: data)
          expect(instance).to be_invalid
          aggregate_failures do
            expect(instance.errors[:base]).to include("ary[0].x can't be nil")
            expect(instance.errors[:base]).to include("ary[0].y can't be nil")
            expect(instance.errors[:base]).to include("ary[1].y can't be nil")
            expect(instance.errors[:base]).to include("ary[2].x can't be nil")
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
            expect(instance.errors[:base]).to include("m[0][1].s can't be nil")
            expect(instance.errors[:base]).to include("m[1][1].s can't be nil")
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
            expect(instance.errors[:base]).to include("map['a'].x can't be nil")
            expect(instance.errors[:base]).to include("map['b'].y can't be nil")
            expect(instance.errors[:base]).to include("map['b'].y can't be nil")
            expect(instance.errors[:base]).to include("map['c'].y can't be nil")
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
            expect(instance.errors[:base]).to include("map['a'].x can't be nil")
            expect(instance.errors[:base]).to include("map['b'].y can't be nil")
            expect(instance.errors[:base]).to include("map['b'].y can't be nil")
            expect(instance.errors[:base]).to include("map['c'].y can't be nil")
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
            expect(instance.errors[:base]).to include("u.x can't be nil")
            expect(instance.errors[:base]).to include("u.y can't be nil")
          end
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

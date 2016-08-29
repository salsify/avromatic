# This examples expects let-variables to be defined for:
#   decoded: a model instance based on the encoded_value
shared_examples_for "logical type encoding and decoding" do
  context "logical types" do
    let(:schema_name) { 'test.logical_types' }
    let(:test_class) do
      Avromatic::Model.model(schema_name: schema_name)
    end
    let(:now) { Time.now }

    with_logical_types do
      context "supported" do
        let(:values) do
          {
            date: Date.today,
            ts_msec: Time.at(now.to_i, now.usec / 1000 * 1000),
            ts_usec: now,
            unknown: 42
          }
        end

        it "encodes and decodes instances" do
          expect(decoded).to eq(instance)
        end
      end
    end

    without_logical_types do
      context "unsupported" do
        let(:values) do
          {
            date: (Date.today - Date.new(1970, 1, 1)).to_i,
            ts_msec: now.to_i + now.usec / 1000 * 1000,
            ts_usec: now.to_i * 1_000_000 + now.usec,
            unknown: 42
          }
        end

        it "encodes and decodes instances" do
          expect(decoded).to eq(instance)
        end
      end
    end
  end
end

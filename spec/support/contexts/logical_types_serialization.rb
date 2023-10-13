# frozen_string_literal: true

# This examples expects let-variables to be defined for:
#   decoded: a model instance based on the encoded_value
shared_examples_for "logical type encoding and decoding" do
  context "logical types" do
    let(:schema_name) { 'test.logical_types_with_decimal' }
    let(:test_class) do
      Avromatic::Model.model(schema_name: schema_name)
    end
    let(:now) do
      # ensure that the Time value has nanoseconds
      time = Time.now
      if time.nsec % 1000 == 0
        Time.at(time.to_i, (time.nsec + rand(999) + 1) / 1000.0)
      else
        time
      end
    end

    with_logical_types do
      context "supported" do
        let(:values) do
          {
            date: Date.today,
            decimal: BigDecimal('5.2'),
            ts_msec: now,
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
        let(:epoch_start) { Date.new(1970, 1, 1) }
        let(:values) do
          {
            date: (Date.today - epoch_start).to_i,
            decimal: BigDecimal('1.5432'),
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

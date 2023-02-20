# frozen_string_literal: true

# This examples expects let-variables to be defined for:
#   decoded: a model instance based on the encoded_value
shared_examples_for "logical type encoding and decoding" do
  context "logical types" do
    let(:schema_name) do
      Avromatic.allow_decimal_logical_type ? 'test.logical_types_with_decimal' : 'test.logical_types'
    end
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
            ts_msec: now,
            ts_usec: now,
            unknown: 42
          }.tap { _1[:decimal] = BigDecimal('5.2') if Avromatic.allow_decimal_logical_type }
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
            ts_msec: now.to_i + now.usec / 1000 * 1000,
            ts_usec: now.to_i * 1_000_000 + now.usec,
            unknown: 42
          }.tap { _1[:decimal] = BigDecimal('1.5432') if Avromatic.allow_decimal_logical_type }
        end

        it "encodes and decodes instances" do
          expect(decoded).to eq(instance)
        end
      end
    end
  end
end

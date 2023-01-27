# frozen_string_literal: true

record :logical_types, namespace: :test do
  required :date, :int, logical_type: 'date'
  required :ts_msec, :long, logical_type: 'timestamp-millis'
  required :ts_usec, :long, logical_type: 'timestamp-micros'
  required :decimal, :bytes, logical_type: 'decimal', precision: 4
  required :unknown, :int, logical_type: 'foobar'
end

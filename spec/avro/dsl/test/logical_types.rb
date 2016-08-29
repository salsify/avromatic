record :logical_types, namespace: :test do
  required :date, :int, logical_type: 'date'
  required :ts_msec, :long, logical_type: 'timestamp-millis'
  required :ts_usec, :long, logical_type: 'timestamp-micros'
  required :unknown, :int, logical_type: 'foobar'
end

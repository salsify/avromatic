# frozen_string_literal: true

namespace :test

record :nested_nested_record do
  # primitive types
  required :n, :null
  required :b, :boolean
  required :i, :int
  required :l, :long
  required :f, :float
  required :d, :double
  required :bs, :bytes
  required :str, :string

  # logical types
  required :date, :int, logical_type: 'date'
  required :ts_msec, :long, logical_type: 'timestamp-millis'
  required :ts_usec, :long, logical_type: 'timestamp-micros'
  required :unknown, :int, logical_type: 'foobar'

  # complex types
  required :e, :enum, symbols: [:A, :B]
  required :a, :array, items: :int
  required :m, :map, values: :int
  required :u, :union, types: [:string, :int]
  required :fx, :fixed, size: 2

  required :sub, :record do
    # primitive types
    required :n, :null
    required :b, :boolean
    required :i, :int
    required :l, :long
    required :f, :float
    required :d, :double
    required :bs, :bytes
    required :str, :string

    # logical types
    required :date, :int, logical_type: 'date'
    required :ts_msec, :long, logical_type: 'timestamp-millis'
    required :ts_usec, :long, logical_type: 'timestamp-micros'
    required :unknown, :int, logical_type: 'foobar'

    # complex types
    required :e, :enum, symbols: [:A, :B]
    required :a, :array, items: :int
    required :m, :map, values: :int
    required :u, :union, types: [:string, :int]
    required :fx, :fixed, size: 2

    required :subsub, :record do
      # primitive types
      required :n, :null
      required :b, :boolean
      required :i, :int
      required :l, :long
      required :f, :float
      required :d, :double
      required :bs, :bytes
      required :str, :string

      # logical types
      required :date, :int, logical_type: 'date'
      required :ts_msec, :long, logical_type: 'timestamp-millis'
      required :ts_usec, :long, logical_type: 'timestamp-micros'
      required :unknown, :int, logical_type: 'foobar'

      # complex types
      required :e, :enum, symbols: [:A, :B]
      required :a, :array, items: :int
      required :m, :map, values: :int
      required :u, :union, types: [:string, :int]
      required :fx, :fixed, size: 2
    end
  end
end

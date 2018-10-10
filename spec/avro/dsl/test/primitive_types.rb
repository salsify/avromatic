# frozen_string_literal: true

namespace :test

record :primitive_types do
  required :s, :string
  required :b, :bytes
  required :tf, :boolean
  required :i, :int
  required :l, :long
  required :f, :float
  required :d, :double
  required :n, :null
  required :fx, :fixed, size: 7
  required :e, :enum, symbols: %i(A B)
end

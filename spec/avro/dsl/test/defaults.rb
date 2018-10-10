# frozen_string_literal: true

namespace 'test'

record :defaults do
  required :defaulted_enum, :enum, symbols: %i(A B), default: :A
  required :defaulted_string, :string, default: 'fnord'
  required :defaulted_int, :int, default: 42
end

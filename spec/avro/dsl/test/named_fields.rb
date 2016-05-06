namespace 'test'

record :named_fields do
  required :f, :fixed, size: 7
  required :e, :enum, symbols: %i(A B)
  required :sub, :record do
    required :s, :string
  end
end

record :with_map do
  required :pairs, :map, values: :int, default: { a: 1 }
end

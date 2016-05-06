record :with_array do
  required :names, :array, items: :string, default: ['first']
end

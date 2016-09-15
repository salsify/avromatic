record :recursive, namespace: :test do
  required :s, :string
  optional :child, :recursive
end

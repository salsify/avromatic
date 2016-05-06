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
end

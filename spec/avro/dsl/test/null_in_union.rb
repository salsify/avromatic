namespace :test

record :i_rec do
  required :i, :int
end

record :s_rec do
  required :s, :string
end

record :null_in_union do
  required :values, array(union(:i_rec, null, :s_rec))
end

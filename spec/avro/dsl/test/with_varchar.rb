namespace :test

record :varchar do
  required :length, :int
  required :data, :bytes
end

record :with_varchar do
  required :str, :varchar
end

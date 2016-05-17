namespace :test

record :nested_record do
  required :str, :string, default: 'A'

  required :sub, :record do
    required :str, :string, default: 'B'
    required :i, :int, default: 0
  end
end

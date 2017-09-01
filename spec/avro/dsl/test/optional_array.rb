namespace :test

record :message do
  required :body, :string
end

record :optional_array do
  required :id, :int
  optional :messages, array(:message)
end

namespace :test

# record type contains fields that are either reserved words
# for Virtus or methods that are used in the model implementation.
record :reserved do
  required :attributes, :array, items: :string
  required :avro_message_value, :string
  required :hash, :map, values: :string
  required :okay, :string, default: ''
end

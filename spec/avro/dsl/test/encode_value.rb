# frozen_string_literal: true

record :encode_value, namespace: :test do
  required :str1, :string, default: 'X'
  required :str2, :string, default: 'Y'
end

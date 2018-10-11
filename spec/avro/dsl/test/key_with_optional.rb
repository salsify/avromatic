# frozen_string_literal: true

record :key_with_optional, namespace: :test do
  required :id, :long
  optional :name, :string
end

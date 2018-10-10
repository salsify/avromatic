# frozen_string_literal: true

record :with_array, namespace: :test do
  required :names, :array, items: :string, default: ['first']
end

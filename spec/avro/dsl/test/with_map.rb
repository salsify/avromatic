# frozen_string_literal: true

record :with_map, namespace: :test do
  required :pairs, :map, values: :int, default: { a: 1 }
end

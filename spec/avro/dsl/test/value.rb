# frozen_string_literal: true

record :value, namespace: :test do
  required :action, :enum, symbols: [:CREATE, :UPDATE, :DESTROY]
  required :id, :long
end

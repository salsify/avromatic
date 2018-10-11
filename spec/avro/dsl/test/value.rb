# frozen_string_literal: true

record :value, namespace: :test do
  required :action, :enum, symbols: %i(CREATE UPDATE DESTROY)
  required :id, :long
end

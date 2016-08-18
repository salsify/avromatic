record :value, namespace: :test do
  required :action, :enum, symbols: %i(CREATE UPDATE DESTROY)
  required :id, :long
end

record :value do
  namespace :test
  required :action, :enum, symbols: %i(CREATE UPDATE DESTROY)
  required :id, :long
end

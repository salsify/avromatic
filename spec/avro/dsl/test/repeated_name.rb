namespace :test

record :sub do
  required :i, :int
end

record :repeated_name do
  required :old, :sub
  required :new, :sub
end

namespace :test

fixed :six, size: 6

record :named_type  do
  required :six_str, :six, default: ''.ljust(6)
  optional :optional_six, :six
end

# frozen_string_literal: true

namespace :test

record :real_int_union do
  required :header, :string
  required :message, :union, types: [:int, :string, :long]
  optional :string_or_long, :union, types: [:string, :long]
end

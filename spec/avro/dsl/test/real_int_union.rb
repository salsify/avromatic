# frozen_string_literal: true

namespace :test

record :real_int_union do
  required :header, :string
  required :message, :union, types: [:int, :string, :long]
end

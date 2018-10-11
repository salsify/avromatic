# frozen_string_literal: true

namespace :test

record :message do
  required :body, :string
end

record :optional_record do
  required :id, :int
  optional :message, :message
end

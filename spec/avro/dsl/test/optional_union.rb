# frozen_string_literal: true

namespace :test

record :foo do
  required :foo_message, :string
end

record :bar do
  required :bar_message, :string
end

record :optional_union do
  required :header, :string
  optional :message, :union, types: [:foo, :bar]
end

# frozen_string_literal: true

namespace :test

record :nested_nested_record do
  required :str, :string, default: 'A'

  required :sub, :record do
    required :str, :string, default: 'B'
    required :i, :int, default: 0

    optional :subsub, :record do
      required :str, :string, default: 'C'
      required :i, :int, default: 42
    end
  end
end

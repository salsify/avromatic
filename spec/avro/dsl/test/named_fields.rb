# frozen_string_literal: true

namespace 'test'

record :named_fields do
  required :sub, :record do
    required :s, :string
  end
end

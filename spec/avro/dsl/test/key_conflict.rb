# frozen_string_literal: true

record :key_conflict, namespace: 'test' do
  required :id, :bytes
  required :b, :string
end

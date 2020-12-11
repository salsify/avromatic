# frozen_string_literal: true

describe Avromatic::IO::UnionDatum do
  describe "#==" do
    it "returns true when the member index and datum are equal" do
      member1 = described_class.new(1, foo: 'bar')
      member2 = described_class.new(1, foo: 'bar')
      expect(member1).to eq(member2)
    end

    it "returns false when the member indexes are not equal" do
      member1 = described_class.new(1, foo: 'bar')
      member2 = described_class.new(2, foo: 'bar')
      expect(member1).not_to eq(member2)
    end

    it "returns false when the datums are not equal" do
      member1 = described_class.new(1, foo: 'bar')
      member2 = described_class.new(1, foo: 'baz')
      expect(member1).not_to eq(member2)
    end
  end

  describe "#hash" do
    it "returns the same value for equal UnionDatums" do
      member1 = described_class.new(1, foo: 'bar')
      member2 = described_class.new(1, foo: 'bar')
      expect(member1.hash).to eq(member2.hash)
    end

    it "returns different values when the member indexes are different" do
      member1 = described_class.new(1, foo: 'bar')
      member2 = described_class.new(2, foo: 'bar')
      expect(member1.hash).not_to eq(member2.hash)
    end

    it "returns different values when the datums are different" do
      member1 = described_class.new(1, foo: 'bar')
      member2 = described_class.new(1, foo: 'baz')
      expect(member1.hash).not_to eq(member2.hash)
    end
  end
end

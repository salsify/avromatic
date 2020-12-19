# frozen_string_literal: true

describe Avromatic::Model::AttributePath do
  it "renders a single attribute access" do
    path = described_class.new('foo')
    expect(path.to_s).to eq('foo')
  end

  it "renders nested attributes access" do
    path = described_class.new('baz').prepend_attribute_access('bar').prepend_attribute_access('foo')
    expect(path.to_s).to eq('foo.bar.baz')
  end

  it "renders array access" do
    path = described_class.new('bar').prepend_array_access(1).prepend_attribute_access('foo')
    expect(path.to_s).to eq('foo[1].bar')
  end

  it "renders nested array access" do
    path = described_class.new('bar').prepend_array_access(2).prepend_array_access(1).prepend_attribute_access('foo')
    expect(path.to_s).to eq('foo[1][2].bar')
  end

  it "renders map access" do
    path = described_class.new('bar').prepend_map_access('key').prepend_attribute_access('foo')
    expect(path.to_s).to eq("foo['key'].bar")
  end

  it "renders nested map access" do
    path = described_class.new('bar').prepend_map_access('key2').prepend_map_access('key1').prepend_attribute_access('foo')
    expect(path.to_s).to eq("foo['key1']['key2'].bar")
  end
end

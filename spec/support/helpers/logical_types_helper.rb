module LogicalTypesHelper

  def with_logical_types
    yield if logical_types?
  end

  def without_logical_types
    yield unless logical_types?
  end

  private

  def logical_types?
    Avro::Schema.instance_methods.include?(:logical_type)
  end
end

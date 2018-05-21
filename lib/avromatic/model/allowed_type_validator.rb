class AllowedTypeValidator < ActiveModel::EachValidator
  def validate_each(record, name, value)
    if options[:in].find { |klass| value.is_a?(klass) }.nil?
      record.errors[name] << "does not have the expected type #{options[:in]}"
    end
  end
end

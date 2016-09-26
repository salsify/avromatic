module Avromatic
  module Model
    module Validation
      extend ActiveSupport::Concern

      module ClassMethods

        # Returns an array of messages
        def validate_nested_value(value)
          case value
          when Avromatic::Model::Attributes
            validate_record_value(value)
          when Array
            value.map.with_index do |element, index|
              validate_nested_value(element).map do |message|
                "[#{index}]#{message}"
              end
            end.flatten
          when Hash
            value.map do |key, map_value|
              validate_nested_value(map_value).map do |message|
                "['#{key}']#{message}"
              end
            end.flatten
          end || []
        end

        private

        def validate_complex(field_name)
          validate do |instance|
            value = instance.send(field_name)
            messages = self.class.validate_nested_value(value)
            messages.each { |message| instance.errors.add(field_name.to_sym, message) }
          end
        end

        def validate_record_value(record)
          if record && record.invalid?
            record.errors.map do |key, message|
              ".#{key} #{message}".gsub(' .', '.')
            end
          end
        end
      end
    end
  end
end

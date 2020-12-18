# frozen_string_literal: true

module Avromatic
  module Model
    class AttributePath
      def initialize(path)
        @path = Array(path)
      end

      def prepend_map_access(key)
        self.class.new(["['#{key}']"] + @path)
      end

      def prepend_array_access(index)
        self.class.new(["[#{index}]"] + @path)
      end

      def prepend_attribute_access(name)
        self.class.new([name] + @path)
      end

      def to_s
        result = String.new(@path.first)
        @path.drop(1).each do |element|
          result << (element.start_with?('[') ? element : ".#{element}")
        end
        result
      end
    end
  end
end

require 'active_support'

module Avromatic
  module Model
    module AllowedWriterMethodsMemoization

      def self.prepended(base)
        base.prepend(ClassMethods)
      end

      def self.included(base)
        base.class_attribute :virtus_object_allowed_writer_methods
      end

      module ClassMethods
        def allowed_writer_methods
          self.class.virtus_object_allowed_writer_methods ||= super
        end
      end
    end
  end
end

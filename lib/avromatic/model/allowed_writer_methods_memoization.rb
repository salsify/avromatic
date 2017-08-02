module Avromatic
  module Model
    module AllowedWriterMethodsMemoization
      def self.included(base)
        base.class_attribute :virtus_object_allowed_writer_methods
        base.prepend(ClassMethods)
      end

      module ClassMethods
        def allowed_writer_methods
          self.class.virtus_object_allowed_writer_methods ||= super
        end
      end
    end
  end
end

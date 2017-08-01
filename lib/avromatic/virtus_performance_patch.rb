require 'virtus'
require 'active_support'

module VirtusValueObjectAllowedWriterMethodsMemoization
  def self.prepended(base)
    class << base
      prepend ClassMethods
    end
  end

  module ClassMethods
    def included(base)
      if base.class == Module
        base.prepend(VirtusValueObjectAllowedWriterMethodsMemoization)
      else
        base.class_eval do
          class_attribute :virtus_object_allowed_writer_methods
        end
      end
      super
    end
  end

  def allowed_writer_methods
    self.class.virtus_object_allowed_writer_methods ||= super
  end
end

Virtus::ValueObject::AllowedWriterMethods.prepend(VirtusValueObjectAllowedWriterMethodsMemoization)

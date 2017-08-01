require 'virtus'
require 'active_support'

module VirtusAllowedInstanceMethodsMemoization
  def self.prepended(base)
    class << base
      prepend ClassMethods
    end
  end

  module ClassMethods
    def included(base)
      # puts 'hi from VirtusAllowedInstanceMethodsMemoization'
      if base.class == Module
        base.prepend(VirtusAllowedInstanceMethodsMemoization)
      else
        base.class_eval do
          class_attribute :allowed_public_instance_methods
        end
      end
      super
    end
  end

  def allowed_methods
    self.class.allowed_public_instance_methods ||= super
  end
end

Virtus::InstanceMethods.prepend(VirtusAllowedInstanceMethodsMemoization)

module VirtusAllowedWriterMethodsExtensionMemoization
  def self.prepended(base)
    class << base
      prepend ClassMethods
    end
  end

  module ClassMethods
    def included(base)
      # puts 'hi from VirtusAllowedWriterMethodsExtensionMemoization'
      if base.class == Module
        base.prepend(VirtusAllowedWriterMethodsExtensionMemoization)
      else
        base.class_eval do
          class_attribute :allowed_writer_methods_extensions
        end
      end
      super
    end
  end

  def allowed_writer_methods
    if !self.respond_to?(:config) || self.config.mutable
      super
    else
      self.class.allowed_writer_methods_extensions ||= super
    end
  end
end

Virtus::Extensions::AllowedWriterMethods.prepend(VirtusAllowedWriterMethodsExtensionMemoization)

module VirtusValueObjectAllowedWriterMethodsMemoization
  def self.prepended(base)
    class << base
      prepend ClassMethods
    end
  end

  module ClassMethods
    def included(base)
      # puts 'hi from VirtusValueObjectAllowedWriterMethodsMemoization'
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

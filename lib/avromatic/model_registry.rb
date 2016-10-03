require 'active_support/core_ext/string/access'

module Avromatic
  # The ModelRegistry class is used to store and fetch nested models by
  # their fullname. An optional namespace prefix can be removed from the full
  # name that is used to store and fetch models.
  class ModelRegistry

    def initialize(remove_namespace_prefix: nil)
      @prefix = remove_namespace_prefix
      @hash = Hash.new
    end

    def clear
      @hash.clear
    end

    def [](fullname)
      @hash.fetch(fullname)
    end

    def register(model)
      raise 'models with a key schema are not supported' if model.key_avro_schema
      name = model.avro_schema.fullname
      name = remove_prefix(name)
      raise "'#{name}' has already been registered" if registered?(name)
      @hash[name] = model
    end

    def registered?(fullname)
      @hash.key?(fullname)
    end

    def remove_prefix(name)
      return name if @prefix.nil?

      value =
        case @prefix
        when String
          name.start_with?(@prefix) ? name.from(@prefix.length) : name
        when Regexp
          name.sub(@prefix, '')
        else
          raise "unsupported `remove_namespace_prefix` value: #{@prefix}"
        end

      value.start_with?('.') ? value.from(1) : value
    end
  end
end

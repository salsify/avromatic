# frozen_string_literal: true

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
    alias_method :fetch, :[]

    def register(model)
      raise 'models with a key schema are not supported' if model.key_avro_schema

      name = model_fullname(model)
      raise "'#{name}' has already been registered" if registered?(name)

      @hash[name] = model
    end

    def registered?(fullname_or_model)
      fullname = fullname_or_model.is_a?(String) ? fullname_or_model : model_fullname(fullname_or_model)
      @hash.key?(fullname)
    end

    def model_fullname(model)
      name = model.avro_schema.fullname
      remove_prefix(name)
    end

    def ensure_registered_model(model)
      name = model_fullname(model)
      if registered?(name)
        existing_model = fetch(name)
        unless existing_model.equal?(model)
          raise "Attempted to replace existing Avromatic model #{model_debug_name(existing_model)} with new model " \
            "#{model_debug_name(model)} as '#{name}'. Perhaps '#{model_debug_name(model)}' needs to be eager loaded " \
            'via the Avromatic eager_load_models setting?'
        end
      else
        register(model)
      end
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

    private

    def model_debug_name(model)
      model.name || model.to_s
    end
  end
end

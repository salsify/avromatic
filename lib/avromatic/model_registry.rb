module Avromatic
  # The ModelRegistry class is used to store and fetch nested models by
  # their fullname. An optional namespace prefix can be removed from the full
  # name that is used to store and fetch models.
  class ModelRegistry

    def initialize(remove_namespace_prefix: nil)
      @prefix = remove_namespace_prefix
      @hash = Hash.new
    end

    def [](fullname)
      @hash.fetch(fullname)
    end

    def register(model)
      raise 'models with a key schema are not supported' if model.key_avro_schema
      name = model.avro_schema.fullname
      name = remove_prefix(name) if @prefix
      @hash[name] = model
    end

    def model?(fullname)
      @hash.key?(fullname)
    end

    private

    def remove_prefix(name)
      value =
        case @prefix
        when String
          name[@prefix.length..-1] if name.start_with?(@prefix)
        when Regexp
          name.sub(@prefix, '')
        else
          raise "unsupported `remove_namespace_prefix` value: #{@prefix}"
        end

      value.start_with?('.') ? value[1..-1] : value
    end
  end
end

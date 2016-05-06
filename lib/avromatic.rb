require 'avromatic/version'
require 'avromatic/model'

module Avromatic
  class << self
    attr_accessor :registry_url, :schema_store, :logger
  end

  self.logger = Logger.new($stdout)

  def self.build_schema_registry
    raise 'Avromatic must be configured with a registry_url' unless registry_url
    AvroTurf::CachedSchemaRegistry.new(
      AvroTurf::SchemaRegistry.new(registry_url, logger: logger))
  end
end

require 'avromatic/railtie' if defined?(Rails)

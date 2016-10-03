require 'avromatic/version'
require 'avromatic/model'
require 'avromatic/model_registry'
require 'avro_turf'
require 'avro_turf/messaging'

module Avromatic
  class << self
    attr_accessor :schema_registry, :registry_url, :schema_store, :logger,
                  :messaging, :type_registry, :nested_models, :on_initialize

    delegate :register_type, to: :type_registry
  end

  self.nested_models = ModelRegistry.new
  self.logger = Logger.new($stdout)
  self.type_registry = Avromatic::Model::TypeRegistry.new

  def self.configure
    yield self
  end

  def self.build_schema_registry
    raise 'Avromatic must be configured with a registry_url' unless registry_url
    AvroTurf::CachedSchemaRegistry.new(
      AvroTurf::SchemaRegistry.new(registry_url, logger: logger)
    )
  end

  def self.build_messaging
    raise 'Avromatic must be configured with a schema_store' unless schema_store
    AvroTurf::Messaging.new(
      registry: schema_registry || build_schema_registry,
      schema_store: schema_store,
      logger: logger
    )
  end

  def self.build_messaging!
    self.messaging = build_messaging
  end

  def self.prepare!
    nested_models.clear
    on_initialize.call if on_initialize
  end
end

require 'avromatic/railtie' if defined?(Rails)

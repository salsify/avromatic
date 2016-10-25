require 'avromatic/version'
require 'avromatic/model'
require 'avromatic/model_registry'
require 'avro_turf'
require 'avro_turf/messaging'
require 'active_support/core_ext/string/inflections'

module Avromatic
  class << self
    attr_accessor :schema_registry, :registry_url, :schema_store, :logger,
                  :messaging, :type_registry, :nested_models

    delegate :register_type, to: :type_registry
  end

  self.nested_models = ModelRegistry.new
  self.logger = Logger.new($stdout)
  self.type_registry = Avromatic::Model::TypeRegistry.new

  def self.configure
    yield self
    preregister_models!
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
    preregister_models!
  end

  def self.preregister_models=(models)
    @preregister_model_names = Array(models).map { |model| model.is_a?(Class) ? model.name : model }
  end

  def self.preregister_models!
    (@preregister_model_names || []).each do |model_name|
      nested_models.register_if_missing(model_name.constantize)
    end
  end
  private_class_method :preregister_models!
end

require 'avromatic/railtie' if defined?(Rails)

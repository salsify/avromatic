require 'avromatic/version'
require 'avro_turf'
require 'avromatic/model'
require 'avromatic/model_registry'
require 'avromatic/messaging'
require 'active_support/core_ext/string/inflections'

module Avromatic
  class << self
    attr_accessor :schema_registry, :registry_url, :schema_store, :logger,
                  :messaging, :type_registry, :nested_models,
                  :use_custom_datum_reader, :use_cacheable_schema_registration

    delegate :register_type, to: :type_registry
  end

  self.nested_models = ModelRegistry.new
  self.logger = Logger.new($stdout)
  self.type_registry = Avromatic::Model::TypeRegistry.new
  self.use_custom_datum_reader = true
  self.use_cacheable_schema_registration = true

  def self.configure
    yield self
    eager_load_models!
  end

  def self.build_schema_registry
    raise 'Avromatic must be configured with a registry_url' unless registry_url
    AvroTurf::CachedSchemaRegistry.new(
      AvroTurf::SchemaRegistry.new(registry_url, logger: logger)
    )
  end

  def self.build_messaging
    raise 'Avromatic must be configured with a schema_store' unless schema_store
    Avromatic::Messaging.new(
      registry: schema_registry || build_schema_registry,
      schema_store: schema_store,
      logger: logger
    )
  end

  def self.build_messaging!
    self.messaging = build_messaging
  end

  # This method is called as a Rails to_prepare hook after the application
  # first initializes during boot-up and prior to each code reloading.
  # For the first call during boot-up we do not want to clear the nested_models.
  def self.prepare!(skip_clear: false)
    unless skip_clear
      nested_models.clear
      schema_store.public_send(:clear) if schema_store && schema_store.respond_to?(:clear)
    end
    eager_load_models!
  end

  def self.eager_load_models=(models)
    @eager_load_model_names = Array(models).map { |model| model.is_a?(Class) ? model.name : model }
  end

  def self.eager_load_models!
    (@eager_load_model_names || []).each do |model_name|
      nested_models.ensure_registered_model(model_name.constantize)
    end
  end
  private_class_method :eager_load_models!
end

require 'avromatic/railtie' if defined?(Rails)

# frozen_string_literal: true

module Avromatic
  class Railtie < Rails::Railtie
    initializer 'avromatic.configure' do
      Avromatic.configure do |config|
        config.logger = Rails.logger
      end

      # Rails calls the to_prepare hook once during boot-up, after running
      # initializers. After the to_prepare call during boot-up, no code will
      # we reloaded, so we need to retain the contents of the nested_models
      # registry.
      #
      # For subsequent calls to to_prepare (in development), the nested_models
      # registry is cleared and repopulated by explicitly referencing the
      # eager_loaded_models.
      first_prepare = true

      Rails.configuration.to_prepare do
        Avromatic.prepare!(skip_clear: first_prepare)
        first_prepare = false
      end
    end
  end
end

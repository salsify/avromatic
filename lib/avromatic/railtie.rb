module Avromatic
  class Railtie < Rails::Railtie
    initializer 'avromatic.configure' do
      Avromatic.configure do |config|
        config.logger = Rails.logger
      end

      first_prepare = true

      Rails.configuration.to_prepare do
        Avromatic.prepare!(skip_clear: first_prepare)
        first_prepare = false
      end
    end
  end
end

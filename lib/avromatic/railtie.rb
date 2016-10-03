module Avromatic
  class Railtie < Rails::Railtie
    initializer 'avromatic.configure' do
      Avromatic.configure do |config|
        config.logger = Rails.logger
      end

      Rails.configuration.to_prepare do
        Avromatic.prepare!
      end
    end
  end
end

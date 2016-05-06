module Avromatic
  class Railtie < Rails::Railtie
    initializer 'avromatic.configure' do
      SalsifyAvro.configure do |config|
        config.logger = Rails.logger
      end
    end
  end
end

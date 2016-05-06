module Avromatic
  class Railtie < Rails::Railtie
    initializer 'avromatic.configure' do
      Avromatic.configure do |config|
        config.logger = Rails.logger
      end
    end
  end
end

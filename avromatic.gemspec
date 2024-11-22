# frozen_string_literal: true

lib = File.expand_path('lib', __dir__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)
require 'avromatic/version'

Gem::Specification.new do |spec|
  spec.name          = 'avromatic'
  spec.version       = Avromatic::VERSION
  spec.authors       = ['Salsify Engineering']
  spec.email         = ['engineering@salsify.com']

  spec.summary       = 'Generate Ruby models from Avro schemas'
  spec.description   = spec.summary
  spec.homepage      = 'https://github.com/salsify/avromatic.git'
  spec.license       = 'MIT'

  if spec.respond_to?(:metadata)
    spec.metadata['allowed_push_host'] = 'https://rubygems.org'
    spec.metadata['rubygems_mfa_required'] = 'true'
  else
    raise 'RubyGems 2.0 or newer is required to set allowed_push_host.'
  end

  spec.files         = `git ls-files -z`.split("\x0").select { |f| f.match(%r{^(bin/|lib/|LICENSE.txt)}) }
  spec.bindir        = 'exe'
  spec.executables   = spec.files.grep(%r{^exe/}) { |f| File.basename(f) }
  spec.require_paths = ['lib']

  spec.metadata['rubygems_mfa_required'] = 'true'

  spec.required_ruby_version = '>= 2.7'

  spec.add_runtime_dependency 'activemodel', '>= 5.2', '< 8.1'
  spec.add_runtime_dependency 'activesupport', '>= 5.2', '< 8.1'
  spec.add_runtime_dependency 'avro', '>= 1.11.0', '< 1.12'
  spec.add_runtime_dependency 'avro_schema_registry-client', '>= 0.4.0'
  spec.add_runtime_dependency 'avro_turf'
  spec.add_runtime_dependency 'ice_nine'

  spec.add_development_dependency 'appraisal'
  spec.add_development_dependency 'avro-builder', '>= 0.12.0'
  spec.add_development_dependency 'bundler', '~> 2.0'
  spec.add_development_dependency 'overcommit', '0.35.0'
  spec.add_development_dependency 'rake', '~> 13.0'
  spec.add_development_dependency 'rspec', '~> 3.8'
  spec.add_development_dependency 'rspec_junit_formatter'
  spec.add_development_dependency 'salsify_rubocop', '~> 1.27.1'
  spec.add_development_dependency 'simplecov'
  spec.add_development_dependency 'webmock'
  # For AvroSchemaRegistry::FakeServer
  spec.add_development_dependency 'sinatra'
end

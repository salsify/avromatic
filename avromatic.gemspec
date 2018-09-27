# coding: utf-8

lib = File.expand_path('../lib', __FILE__)
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

  spec.files         = `git ls-files -z`.split("\x0").reject { |f| f.match(%r{^(test|spec|features)/}) }
  spec.bindir        = 'exe'
  spec.executables   = spec.files.grep(%r{^exe/}) { |f| File.basename(f) }
  spec.require_paths = ['lib']

  spec.add_runtime_dependency 'avro', '>= 1.7.7'
  spec.add_runtime_dependency 'activesupport', '>= 4.1', '< 5.3'
  spec.add_runtime_dependency 'activemodel', '>= 4.1', '< 5.3'
  spec.add_runtime_dependency 'avro_turf'
  spec.add_runtime_dependency 'avro_schema_registry-client', '>= 0.3.0'
  spec.add_runtime_dependency 'ice_nine'

  spec.add_development_dependency 'bundler', '~> 1.11'
  spec.add_development_dependency 'rake', '~> 10.0'
  spec.add_development_dependency 'rspec', '~> 3.0'
  spec.add_development_dependency 'simplecov'
  spec.add_development_dependency 'webmock'
  spec.add_development_dependency 'avro-builder', '>= 0.12.0'
  # For AvroSchemaRegistry::FakeServer
  spec.add_development_dependency 'sinatra'
  spec.add_development_dependency 'salsify_rubocop', '~> 0.48.0'
  spec.add_development_dependency 'overcommit', '0.35.0'
  spec.add_development_dependency 'appraisal'
end

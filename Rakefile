# frozen_string_literal: true

require 'bundler/gem_tasks'
require 'rspec/core/rake_task'
require 'appraisal/task'
require 'avro/builder'

RSpec::Core::RakeTask.new(:default_spec)

Appraisal::Task.new

# rubocop:disable Rake/DuplicateTask
if !ENV['APPRAISAL_INITIALIZED']
  task default: :appraisal
  task spec: :appraisal # rubocop:disable Rake/Desc
else
  task default: :default_spec
end
# rubocop:enable Rake/DuplicateTask

namespace :avro do
  desc 'Generate Avro schema files used by specs'
  task :generate_spec do
    root = 'spec/avro/dsl'
    Avro::Builder.add_load_path(root)
    Dir["#{root}/**/*.rb"].each do |dsl_file|
      puts "Generating Avro schema from #{dsl_file}"
      output_file = dsl_file.sub('/dsl/', '/schema/').sub(/\.rb$/, '.avsc')
      schema = Avro::Builder.build(File.read(dsl_file))
      FileUtils.mkdir_p(File.dirname(output_file))
      File.write(output_file, schema.end_with?("\n") ? schema : schema << "\n")
    end
  end
end

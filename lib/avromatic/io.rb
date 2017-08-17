module Avromatic
  module IO
    UNION_MEMBER_INDEX = '__avromatic_member_index'.freeze
    ENCODING_PROVIDER = '__avromatic_encoding_provider'.freeze
  end
end

require 'avromatic/io/datum_reader'
require 'avromatic/io/datum_writer'

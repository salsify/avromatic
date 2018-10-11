# frozen_string_literal: true

module Avromatic
  module IO
    UNION_MEMBER_INDEX = '__avromatic_member_index'
    ENCODING_PROVIDER = '__avromatic_encoding_provider'
  end
end

require 'avromatic/io/datum_reader'
require 'avromatic/io/datum_writer'

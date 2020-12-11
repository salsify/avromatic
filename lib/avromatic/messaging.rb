# frozen_string_literal: true

require 'avro_turf/messaging'
require 'avromatic/io'

module Avromatic
  # Subclass AvroTurf::Messaging to use a custom DatumReader and DatumWriter
  class Messaging < AvroTurf::Messaging
    attr_reader :registry

    def decode(data, schema_name: nil, namespace: @namespace)
      readers_schema = schema_name && @schema_store.find(schema_name, namespace)
      stream = StringIO.new(data)
      decoder = Avro::IO::BinaryDecoder.new(stream)

      # The first byte is MAGIC!!!
      magic_byte = decoder.read(1)

      if magic_byte != MAGIC_BYTE
        raise "Expected data to begin with a magic byte, got `#{magic_byte.inspect}`"
      end

      # The schema id is a 4-byte big-endian integer.
      schema_id = decoder.read(4).unpack('N').first

      writers_schema = @schemas_by_id.fetch(schema_id) do
        schema_json = @registry.fetch(schema_id)
        @schemas_by_id[schema_id] = Avro::Schema.parse(schema_json)
      end

      # The following line differs from the parent class to use a custom DatumReader
      reader_class = Avromatic.use_custom_datum_reader ? Avromatic::IO::DatumReader : Avro::IO::DatumReader
      reader = reader_class.new(writers_schema, readers_schema)
      reader.read(decoder)
    end

    def encode(message, schema_name: nil, namespace: @namespace, subject: nil)
      schema = @schema_store.find(schema_name, namespace)

      # Schemas are registered under the full name of the top level Avro record
      # type, or `subject` if it's provided.
      schema_id = @registry.register(subject || schema.fullname, schema)

      stream = StringIO.new
      encoder = Avro::IO::BinaryEncoder.new(stream)

      # Always start with the magic byte.
      encoder.write(MAGIC_BYTE)

      # The schema id is encoded as a 4-byte big-endian integer.
      encoder.write([schema_id].pack('N'))

      # The following line differs from the parent class to use a custom DatumWriter
      writer_class = Avromatic.use_custom_datum_writer ? Avromatic::IO::DatumWriter : Avro::IO::DatumWriter
      writer = writer_class.new(schema)

      # The actual message comes last.
      writer.write(message, encoder)

      stream.string
    end
  end
end

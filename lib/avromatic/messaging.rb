require 'avro_turf/messaging'
require 'avromatic/io/datum_reader'

module Avromatic
  # Subclass AvroTurf::Messaging to use a custom DatumReader for decode.
  class Messaging < AvroTurf::Messaging
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
      reader = Avromatic::IO::DatumReader.new(writers_schema, readers_schema)
      reader.read(decoder)
    end
  end
end

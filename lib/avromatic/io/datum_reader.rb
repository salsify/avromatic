# frozen_string_literal: true

module Avromatic
  module IO
    # Subclass DatumReader to include additional information about the union
    # member index used. The code modified below is based on salsify/avro,
    # branch 'salsify-master' with the tag 'v1.9.0.3'
    class DatumReader < Avro::IO::DatumReader

      def read_data(writers_schema, readers_schema, decoder)
        # schema resolution: reader's schema is a union, writer's schema is not
        return super unless writers_schema.type_sym != :union && readers_schema.type_sym == :union

        rs_index = readers_schema.schemas.find_index do |s|
          self.class.match_schemas(writers_schema, s)
        end

        raise Avro::IO::SchemaMatchException.new(writers_schema, readers_schema) unless rs_index

        datum = read_data(writers_schema, readers_schema.schemas[rs_index], decoder)
        optional = readers_schema.schemas.first.type_sym == :null

        if readers_schema.schemas.size == 2 && optional
          # Avromatic does not treat the union of null and 1 other type as a union
          datum
        elsif datum.nil?
          # Avromatic does not treat the null of an optional field as part of the union
          nil
        else
          # Avromatic does not treat the null of an optional field as part of the union so
          # adjust the member index accordingly
          member_index = optional ? rs_index - 1 : rs_index
          Avromatic::IO::UnionDatum.new(member_index, datum)
        end
      end
    end
  end
end
# rubocop:enable Style/WhenThen

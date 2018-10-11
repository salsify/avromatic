# frozen_string_literal: true

module Avromatic
  module IO
    # Subclass DatumWriter to use additional information about union member
    # index.
    class DatumWriter < Avro::IO::DatumWriter
      def write_union(writers_schema, datum, encoder)
        optional = writers_schema.schemas.first.type_sym == :null
        if datum.is_a?(Hash) && datum.key?(Avromatic::IO::UNION_MEMBER_INDEX)
          index_of_schema = datum[Avromatic::IO::UNION_MEMBER_INDEX]
          # Avromatic does not treat the null of an optional field as part of the union
          index_of_schema += 1 if optional
        elsif optional && writers_schema.schemas.size == 2
          # Optimize for the common case of a union that's just an optional field
          index_of_schema = datum.nil? ? 0 : 1
        else
          index_of_schema = writers_schema.schemas.find_index do |schema|
            Avro::Schema.validate(schema, datum)
          end
        end
        unless index_of_schema
          raise Avro::IO::AvroTypeError.new(writers_schema, datum)
        end
        encoder.write_long(index_of_schema)
        write_data(writers_schema.schemas[index_of_schema], datum, encoder)
      end

      def write_record(writers_schema, datum, encoder)
        if datum.is_a?(Hash) && datum.key?(Avromatic::IO::ENCODING_PROVIDER)
          encoder.write(datum[Avromatic::IO::ENCODING_PROVIDER].avro_raw_value)
        else
          super
        end
      end
    end
  end
end

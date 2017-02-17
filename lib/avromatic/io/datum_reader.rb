# rubocop:disable Style/WhenThen
module Avromatic
  module IO
    # Subclass DatumReader to include additional information about the union
    # member index used. The code modified below is based on salsify/avro,
    # branch 'salsify-master' with the tag 'v1.9.0.3'
    class DatumReader < Avro::IO::DatumReader

      UNION_MEMBER_INDEX = '__avromatic_member_index'.freeze

      def read_data(writers_schema, readers_schema, decoder, initial_record = {})
        # schema matching
        unless self.class.match_schemas(writers_schema, readers_schema)
          raise Avro::IO::SchemaMatchException.new(writers_schema, readers_schema)
        end

        # schema resolution: reader's schema is a union, writer's schema is not
        if writers_schema.type_sym != :union && readers_schema.type_sym == :union
          rs_index = readers_schema.schemas.find_index do |s|
            self.class.match_schemas(writers_schema, s)
          end

          union_info = { UNION_MEMBER_INDEX => rs_index }

          return read_data(writers_schema, readers_schema.schemas[rs_index], decoder, union_info) if rs_index
          raise Avro::IO::SchemaMatchException.new(writers_schema, readers_schema)
        end

        # function dispatch for reading data based on type of writer's schema
        datum = case writers_schema.type_sym
                when :null;    decoder.read_null
                when :boolean; decoder.read_boolean
                when :string;  decoder.read_string
                when :int;     decoder.read_int
                when :long;    decoder.read_long
                when :float;   decoder.read_float
                when :double;  decoder.read_double
                when :bytes;   decoder.read_bytes
                when :fixed;   read_fixed(writers_schema, readers_schema, decoder)
                when :enum;    read_enum(writers_schema, readers_schema, decoder)
                when :array;   read_array(writers_schema, readers_schema, decoder)
                when :map;     read_map(writers_schema, readers_schema, decoder)
                when :union;   read_union(writers_schema, readers_schema, decoder)
                when :record, :error, :request; read_record(writers_schema, readers_schema, decoder, initial_record)
                else
                  raise Avro::AvroError.new("Cannot read unknown schema type: #{writers_schema.type}")
                end

        # Allow this code to be used with an official Avro release or the
        # avro-salsify-fork that includes logical_type support.
        if readers_schema.respond_to?(:logical_type)
          readers_schema.type_adapter.decode(datum)
        else
          datum
        end
      end

      # Override to specify an initial record that may contain union index
      def read_record(writers_schema, readers_schema, decoder, initial_record = {})
        readers_fields_hash = readers_schema.fields_hash
        read_record = Avromatic.use_custom_datum_reader ? initial_record : {}
        writers_schema.fields.each do |field|
          readers_field = readers_fields_hash[field.name]
          if readers_field
            field_val = read_data(field.type, readers_field.type, decoder)
            read_record[field.name] = field_val
          else
            skip_data(field.type, decoder)
          end
        end

        read_record
      end
    end
  end
end

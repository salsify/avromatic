#include <encoder.hpp>
#include <common.hpp>

#include <iostream>

#include <ruby/encoding.h>

#include <avro/Decoder.hh>
#include <avro/Generic.hh>
#include <avro/GenericDatum.hh>
#include <avro/Specific.hh>

static VALUE rb_avromatic_native_module;
static VALUE rb_union_datum_class;

VALUE utf8_string_to_ruby(const std::string& value) {
  static rb_encoding* encoding = rb_enc_find("UTF-8");
  return rb_enc_str_new(value.c_str(), value.size(), encoding);
}

VALUE deserialize_logical_date(int value) {
  VALUE rb_epoch_start = rb_const_get(rb_avromatic_native_module, rb_intern("EPOCH_START"));
  return rb_funcall(rb_epoch_start, rb_intern("+"), 1, INT2NUM(value));
}

VALUE deserialize_logical_timestamp_millis(long value) {
  long seconds = value / 1000;
  long milliseconds = value % 1000;
  VALUE time_class = rb_const_get(rb_cObject, rb_intern("Time"));
  VALUE rb_value = rb_funcall(time_class, rb_intern("at"), 2, LONG2NUM(seconds), LONG2NUM(milliseconds * 1000));
  return rb_funcall(rb_value, rb_intern("utc"), 0);
}

VALUE deserialize_logical_timestamp_micros(long value) {
  long seconds = value / 1000000;
  long microseconds = value % 1000000;
  VALUE time_class = rb_const_get(rb_cObject, rb_intern("Time"));
  VALUE rb_value = rb_funcall(time_class, rb_intern("at"), 2, LONG2NUM(seconds), LONG2NUM(microseconds));
  return rb_funcall(rb_value, rb_intern("utc"), 0);
}

bool is_optional_field(const avro::NodePtr& schema) {
  return schema->type() == avro::AVRO_UNION &&
    schema->leafAt(0)->type() == avro::AVRO_NULL &&
    schema->leaves() == 2;
}

// Unfortunately we need to track the schema for unions due to https://issues.apache.org/jira/browse/AVRO-2597
VALUE datum_to_ruby_value(const avro::NodePtr& schema, const avro::GenericDatum& datum, bool strict) {
  VALUE rb_value;
  switch (datum.type()) {
    case avro::AVRO_STRING:
    {
      const std::string& field_value = datum.value<std::string>();
      rb_value = utf8_string_to_ruby(field_value);
      break;
    }
    case avro::AVRO_BYTES:
    {
      const std::vector<uint8_t>& bytes = datum.value<std::vector<uint8_t> >();
      rb_value = rb_str_new((const char*)&bytes[0], bytes.size());
      break;
    }
    case avro::AVRO_INT:
    {
      if (datum.logicalType().type() == avro::LogicalType::DATE) {
        rb_value = deserialize_logical_date(datum.value<int>());
      } else {
        rb_value = INT2NUM(datum.value<int>());
      }
      break;
    }
    case avro::AVRO_LONG:
    {
      if (datum.logicalType().type() == avro::LogicalType::TIMESTAMP_MILLIS) {
        rb_value = deserialize_logical_timestamp_millis(datum.value<long>());
      } else if (datum.logicalType().type() == avro::LogicalType::TIMESTAMP_MICROS) {
        rb_value = deserialize_logical_timestamp_micros(datum.value<long>());
      } else {
        rb_value = LONG2NUM(datum.value<long>());
      }
      break;
    }
    case avro::AVRO_FLOAT:
    {
      rb_value = DBL2NUM(datum.value<float>());
      break;
    }
    case avro::AVRO_DOUBLE:
    {
      rb_value = DBL2NUM(datum.value<double>());
      break;
    }
    case avro::AVRO_BOOL:
    {
      rb_value = datum.value<bool>() ? Qtrue : Qfalse;
      break;
    }
    case avro::AVRO_NULL:
    {
      rb_value = Qnil;
      break;
    }
    case avro::AVRO_RECORD:
    {
      rb_value = rb_hash_new();
      const avro::GenericRecord& record_value = datum.value<avro::GenericRecord>();
      const int num_fields = (int)record_value.fieldCount();
      for (int i=0; i<num_fields; ++i) {
        const std::string& field_name = record_value.schema()->nameAt(i);
        VALUE rb_field_name = ID2SYM(rb_intern(field_name.c_str()));
        const avro::GenericDatum& field_datum = record_value.fieldAt(i);
        rb_hash_aset(rb_value, rb_field_name, datum_to_ruby_value(record_value.schema()->leafAt(i), field_datum, strict));
      }
      break;
    }
    case avro::AVRO_ENUM:
    {
      const std::string& symbol = datum.value<avro::GenericEnum>().symbol();
      rb_value = ID2SYM(rb_intern(symbol.c_str()));
      break;
    }
    case avro::AVRO_ARRAY:
    {
      const avro::GenericArray::Value& array_value = datum.value<avro::GenericArray>().value();
      const long num_elements = array_value.size();
      rb_value = rb_ary_new_capa(num_elements);
      for (long i=0; i<num_elements; ++i) {
        rb_ary_store(rb_value, i, datum_to_ruby_value(schema->leafAt(0), array_value[i], strict));
      }
      break;
    }
    case avro::AVRO_MAP:
    {
      rb_value = rb_hash_new();
      const avro::GenericMap::Value& map_entries = datum.value<avro::GenericMap>().value();
      for (auto iter = map_entries.cbegin(); iter != map_entries.cend(); ++iter) {
        VALUE rb_map_key = utf8_string_to_ruby(iter->first);
        VALUE rb_map_value = datum_to_ruby_value(schema->leafAt(1), iter->second, strict);
        rb_hash_aset(rb_value, rb_map_key, rb_map_value);
      }
     break;
    }
    case avro::AVRO_FIXED:
    {
      const avro::GenericFixed& fixedValue = datum.value<avro::GenericFixed>();
      const std::vector<uint8_t>& bytes = fixedValue.value();
      rb_value = rb_str_new((const char*)&bytes[0], bytes.size());
      break;
    }
    default:
      rb_raise(rb_eRuntimeError, "Unexpected field type %s", avro::toString(datum.type()).c_str());
      // Make the compiler happy
      rb_value = Qnil;
  }

  if (!strict && schema->type() == avro::AVRO_UNION && !is_optional_field(schema) && datum.type() != avro::AVRO_NULL) {
    long member_index = schema->leafAt(0)->type() == avro::AVRO_NULL ? datum.unionBranch() - 1 : datum.unionBranch();
    return rb_funcall(rb_union_datum_class, rb_intern("new"), 2, LONG2NUM(member_index), rb_value);
  } else {
    return rb_value;
  }
}

VALUE decode_attributes(VALUE self, VALUE rb_data, VALUE rb_reader_schema, VALUE rb_writer_schema, VALUE rb_strict) {
  const char* raw_bytes = StringValuePtr(rb_data);
  const size_t num_bytes = RSTRING_LEN(rb_data);
  std::unique_ptr<avro::InputStream> inputStream = avro::memoryInputStream((uint8_t*)raw_bytes, num_bytes);

  const avro::ValidSchema* reader_schema = get_cached_schema(rb_reader_schema);
  avro::DecoderPtr decoder = avro::validatingDecoder(*reader_schema, avro::binaryDecoder());
  if (rb_reader_schema != rb_writer_schema) {
    const avro::ValidSchema* writer_schema = get_cached_schema(rb_writer_schema);
    decoder = avro::resolvingDecoder(*writer_schema, *reader_schema, decoder);
  }
  decoder->init(*inputStream);

  avro::GenericDatum datum(*reader_schema);
  avro::decode(*decoder, datum);

  return datum_to_ruby_value(reader_schema->root(), datum, rb_strict == Qtrue);
}

extern
void init_avromatic_decoder(VALUE avromatic_native) {
  rb_avromatic_native_module = avromatic_native;
  rb_define_singleton_method(avromatic_native, "decode_attributes", RB_FUNC(decode_attributes), 4);

  VALUE rb_avromatic_module = rb_const_get(rb_cObject, rb_intern("Avromatic"));
  VALUE rb_model_module = rb_const_get(rb_avromatic_module, rb_intern("IO"));
  rb_union_datum_class = rb_const_get(rb_model_module, rb_intern("UnionDatum"));
}

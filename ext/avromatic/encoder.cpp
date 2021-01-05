#include <encoder.hpp>
#include <common.hpp>

#include <iostream>

#include <avro/Encoder.hh>
#include <avro/Generic.hh>
#include <avro/GenericDatum.hh>
#include <avro/Specific.hh>

static VALUE rb_avromatic_native_module;
static VALUE rb_avromatic_custom_type_class;
static VALUE rb_avromatic_union_type_class;
static VALUE rb_avromatic_validation_error_class;

class MissingRequiredAttributeException : public virtual std::runtime_error {
  public:
    MissingRequiredAttributeException(const std::string &msg) :
        std::runtime_error(msg)
    { }
};

void ruby_model_to_datum(VALUE model, avro::GenericDatum& datum, bool is_value_schema = true);

bool is_union_type(VALUE rb_avromatic_type) {
  return rb_obj_is_kind_of(rb_avromatic_type, rb_avromatic_union_type_class) == Qtrue;
}

VALUE serialize_custom_type(VALUE rb_value, VALUE rb_avromatic_type) {
  if (rb_value != Qnil && rb_obj_is_kind_of(rb_avromatic_type, rb_avromatic_custom_type_class) == Qtrue) {
    return rb_funcall(rb_avromatic_type, rb_intern("serialize"), 2, rb_value, Qtrue);
  } else {
    return rb_value;
  }
}

int serialize_logical_date(VALUE rb_value) {
  VALUE numeric_class = rb_const_get(rb_cObject, rb_intern("Numeric"));
  if (rb_obj_is_kind_of(rb_value, numeric_class) == Qtrue) {
    return NUM2INT(rb_funcall(rb_value, rb_intern("to_i"), 0));
  } else {
    VALUE rb_epoch_start = rb_const_get(rb_avromatic_native_module, rb_intern("EPOCH_START"));
    VALUE rb_time_since_epoch = rb_funcall(rb_value, rb_intern("-"), 1, rb_epoch_start);
    return NUM2INT(rb_funcall(rb_time_since_epoch, rb_intern("to_i"), 0));
  }
}

long serialize_logical_timestamp_millis(VALUE rb_value) {
  VALUE numeric_class = rb_const_get(rb_cObject, rb_intern("Numeric"));
  if (rb_obj_is_kind_of(rb_value, numeric_class) == Qtrue) {
    return NUM2LONG(rb_funcall(rb_value, rb_intern("to_i"), 0));
  } else {
    VALUE rb_time = rb_funcall(rb_value, rb_intern("to_time"), 0);
    long seconds = NUM2LONG(rb_funcall(rb_time, rb_intern("to_i"), 0));
    long microseconds = NUM2LONG(rb_funcall(rb_time, rb_intern("usec"), 0));
    return seconds * 1000 + microseconds / 1000;
  }
}

long serialize_logical_timestamp_micros(VALUE rb_value) {
  VALUE numeric_class = rb_const_get(rb_cObject, rb_intern("Numeric"));
  if (rb_obj_is_kind_of(rb_value, numeric_class) == Qtrue) {
    return NUM2LONG(rb_funcall(rb_value, rb_intern("to_i"), 0));
  } else {
    VALUE rb_time = rb_funcall(rb_value, rb_intern("to_time"), 0);
    long seconds = NUM2LONG(rb_funcall(rb_time, rb_intern("to_i"), 0));
    long microseconds = NUM2LONG(rb_funcall(rb_time, rb_intern("usec"), 0));
    return seconds *  1000000 + microseconds;
  }
}

void ruby_value_to_datum(VALUE rb_value, VALUE rb_avromatic_type, avro::GenericDatum& datum) {
  // TODO: This doesn't support custom records that return record types
  rb_value = serialize_custom_type(rb_value, rb_avromatic_type);

  if (rb_value == Qnil && datum.type() != avro::AVRO_NULL) {
    throw MissingRequiredAttributeException("Unexpected nil field value for type " + avro::toString(datum.type()));
  }

  // Make sure we select the correct branch of the union. Avromatic only allows nils as the first branch of a union
  // so there's nothing to do if we're encoding a null
  if (datum.isUnion() && rb_value != Qnil) {
    if (!is_union_type(rb_avromatic_type)) {
      // Avromatic doesn't model optional fields as unions even though they're unions in the Avro schema
      // so rb_avromatic_type will be the actual member type
      datum.selectBranch(1);
    } else {
      int avromatic_member_index = NUM2INT(rb_funcall(rb_avromatic_type, rb_intern("find_index"), 1, rb_value));
      // Avromatic doesn't include the null member in the member index
      int member_index = rb_funcall(rb_avromatic_type, rb_intern("nullable?"), 0) == Qtrue ? avromatic_member_index + 1 : avromatic_member_index;
      datum.selectBranch(member_index);

      VALUE rb_avromatic_member_types = rb_funcall(rb_avromatic_type, rb_intern("member_types"), 0);
      rb_avromatic_type = rb_ary_entry(rb_avromatic_member_types, avromatic_member_index);
    }
  }

  switch (datum.type()) {
    case avro::AVRO_STRING:
    {
      VALUE rb_utf8_string = rb_funcall(rb_value, rb_intern("encode"), 1, rb_id2str(rb_intern("utf-8")));
      // TODO: Does this work correctly with utf-8 characters?
      datum.value<std::string>() = StringValueCStr(rb_utf8_string);
      break;
    }
    case avro::AVRO_BYTES:
    {
      const char* raw_bytes = StringValuePtr(rb_value);
      const size_t num_bytes = RSTRING_LEN(rb_value);
      std::vector<uint8_t>& destination_buffer = datum.value<std::vector<uint8_t> >();
      destination_buffer.reserve(num_bytes);
      destination_buffer.assign(raw_bytes, raw_bytes + num_bytes);
      break;
    }
    case avro::AVRO_INT:
    {
      if (datum.logicalType().type() == avro::LogicalType::DATE) {
        datum.value<int>() = serialize_logical_date(rb_value);
      } else {
        datum.value<int>() = NUM2INT(rb_value);
      }
      break;
    }
    case avro::AVRO_LONG:
    {
      if (datum.logicalType().type() == avro::LogicalType::TIMESTAMP_MILLIS) {
        datum.value<long>() = serialize_logical_timestamp_millis(rb_value);
      } else if (datum.logicalType().type() == avro::LogicalType::TIMESTAMP_MICROS) {
        datum.value<long>() = serialize_logical_timestamp_micros(rb_value);
      } else {
        datum.value<long>() = NUM2LONG(rb_value);
      }
      break;
    }
    case avro::AVRO_FLOAT:
    {
      datum.value<float>() = (float)RFLOAT_VALUE(rb_value);
      break;
    }
    case avro::AVRO_DOUBLE:
    {
      datum.value<double>() = RFLOAT_VALUE(rb_value);
      break;
    }
    case avro::AVRO_BOOL:
    {
      datum.value<bool>() = rb_value == Qtrue;
      break;
    }
    case avro::AVRO_NULL:
    {
      // Nothing todo here
      break;
    }
    case avro::AVRO_RECORD:
    {
      ruby_model_to_datum(rb_value, datum);
      break;
    }
    case avro::AVRO_ENUM:
    {
      datum.value<avro::GenericEnum>().set(StringValueCStr(rb_value));
      break;
    }
    case avro::AVRO_ARRAY:
    {
      avro::GenericArray& arrayValue = datum.value<avro::GenericArray>();
      VALUE rb_avromatic_item_type = rb_funcall(rb_avromatic_type, rb_intern("value_type"), 0);
      const long num_values = rb_array_len(rb_value);
      arrayValue.value().resize(num_values, avro::GenericDatum(arrayValue.schema()->leafAt(0)));
      for (long i=0; i<num_values; ++i) {
        VALUE rb_nested_value = rb_ary_entry(rb_value, i);
        ruby_value_to_datum(rb_nested_value, rb_avromatic_item_type, arrayValue.value()[i]);
      }
      break;
    }
    case avro::AVRO_MAP:
    {
      avro::GenericMap& mapValue = datum.value<avro::GenericMap>();
      VALUE rb_avromatic_item_type = rb_funcall(rb_avromatic_type, rb_intern("value_type"), 0);
      // TODO: Avoid allocating the key array by using rb_hash_foreach
      VALUE rb_keys = rb_funcall(rb_value, rb_intern("keys"), 0);
      const long num_keys = rb_array_len(rb_keys);
      mapValue.value().resize(num_keys, std::pair<std::string, avro::GenericDatum>("", avro::GenericDatum(mapValue.schema()->leafAt(1))));
      for (long i=0; i<num_keys; ++i) {
        VALUE rb_key = rb_ary_entry(rb_keys, i);
        VALUE rb_utf8_key = rb_funcall(rb_key, rb_intern("encode"), 1, rb_id2str(rb_intern("utf-8")));
        // TODO: Does this work correctly with utf-8 characters?
        mapValue.value()[i].first = StringValueCStr(rb_utf8_key);

        VALUE rb_nested_value = rb_hash_aref(rb_value, rb_key);
        ruby_value_to_datum(rb_nested_value, rb_avromatic_item_type, mapValue.value()[i].second);
      }
      break;
    }
    case avro::AVRO_FIXED:
    {
      const char* raw_bytes = StringValuePtr(rb_value);
      const size_t num_bytes = RSTRING_LEN(rb_value);
      avro::GenericFixed& fixedValue = datum.value<avro::GenericFixed>();
      fixedValue.value().assign(raw_bytes, raw_bytes + num_bytes);
      break;
    }
    default:
      rb_raise(rb_eRuntimeError, "Unexpected field type %s", avro::toString(datum.type()).c_str());
  }
}

void ruby_model_to_datum(VALUE rb_model, avro::GenericDatum& datum, bool is_value_schema) {
  avro::GenericRecord& recordDatum = datum.value<avro::GenericRecord>();
  VALUE rb_attributes = rb_funcall(rb_model, rb_intern("_attributes"), 0);
  VALUE rb_attribute_definitions = rb_funcall(rb_model, rb_intern("attribute_definitions"), 0);
  VALUE rb_field_names = is_value_schema ?
    rb_funcall(rb_model, rb_intern("value_avro_field_names"), 0) :
    rb_funcall(rb_model, rb_intern("key_avro_field_names"), 0);
  const long num_fields = rb_array_len(rb_field_names);
  for (long i=0; i<num_fields; ++i) {
    VALUE rb_field_name = rb_ary_entry(rb_field_names, i);
    VALUE rb_field_value = rb_hash_aref(rb_attributes, rb_field_name);
    VALUE rb_attribute_definition = rb_hash_aref(rb_attribute_definitions, rb_field_name);
    VALUE rb_avromatic_type = rb_funcall(rb_attribute_definition, rb_intern("type"), 0);
    const int field_index = (int)recordDatum.fieldIndex(rb_id2name(SYM2ID(rb_field_name)));
    ruby_value_to_datum(rb_field_value, rb_avromatic_type, recordDatum.fieldAt(field_index));
  }
}

VALUE encode_model(VALUE self, VALUE rb_model, VALUE rb_is_value_schema) {
  bool is_value_schema = rb_is_value_schema == Qtrue;
  VALUE rb_avro_schema = rb_funcall(rb_model, is_value_schema ? rb_intern("value_avro_schema") : rb_intern("key_avro_schema"), 0);
  const avro::ValidSchema* schema = get_cached_schema(rb_avro_schema);

  avro::GenericDatum datum(schema->root());
  try {
    ruby_model_to_datum(rb_model, datum, is_value_schema);
  } catch (MissingRequiredAttributeException& e) {
    // Trigger a full validation to get all error messages
    rb_funcall(rb_model, rb_intern("avro_validate!"), 0);
    // The line above should raise an exception but just in case...
    rb_raise(rb_avromatic_validation_error_class, "%s", e.what());
  }

  std::unique_ptr<avro::OutputStream> outputStream = avro::memoryOutputStream();
  avro::EncoderPtr encoder = avro::validatingEncoder(*schema, avro::binaryEncoder());
  encoder->init(*outputStream);
  try {
    avro::encode(*encoder, datum);
  } catch (const avro::Exception &e) {
    // This should never happen since the model should be valid at this point but just in case
    rb_raise(rb_avromatic_validation_error_class, "%s", e.what());
  }

  std::shared_ptr<std::vector<uint8_t> > outputStreamSnapshot = avro::snapshot(*outputStream);
  return rb_str_new((char*)&(outputStreamSnapshot->at(0)), outputStreamSnapshot->size());
}

extern
void init_avromatic_encoder(VALUE avromatic_native) {
  rb_avromatic_native_module = avromatic_native;
  rb_define_singleton_method(rb_avromatic_native_module, "encode_model", RB_FUNC(encode_model), 2);

  VALUE rb_avromatic_module = rb_const_get(rb_cObject, rb_intern("Avromatic"));
  VALUE rb_model_module = rb_const_get(rb_avromatic_module, rb_intern("Model"));
  VALUE rb_types_module = rb_const_get(rb_model_module, rb_intern("Types"));
  rb_avromatic_union_type_class = rb_const_get(rb_types_module, rb_intern("UnionType"));
  rb_avromatic_custom_type_class = rb_const_get(rb_types_module, rb_intern("CustomType"));
  rb_avromatic_validation_error_class = rb_const_get(rb_model_module, rb_intern("ValidationError"));
}

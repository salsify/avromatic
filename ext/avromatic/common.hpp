#ifndef AVROMATIC_COMMON
#define AVROMATIC_COMMON

#include <ruby.h>

namespace avro {
  class ValidSchema;
}

#define RB_FUNC(FUNCTION) reinterpret_cast<VALUE(*)(...)>(FUNCTION)

const avro::ValidSchema* get_cached_schema(VALUE rb_avro_schema);

#endif

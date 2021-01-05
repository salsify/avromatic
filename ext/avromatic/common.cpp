#include <common.hpp>
#include <avro/Compiler.hh>
#include <avro/ValidSchema.hh>
#include <iostream>

void native_schema_free(void* data) {
  delete (avro::ValidSchema*)data;
}

size_t native_schema_size(const void* data) {
  return sizeof(avro::ValidSchema);
}

static const rb_data_type_t native_schema_type = {
  .wrap_struct_name = "native_schema",
  .function = {
    .dmark = NULL,
    .dfree = native_schema_free,
    .dsize = native_schema_size,
  },
  .data = NULL,
  .flags = RUBY_TYPED_FREE_IMMEDIATELY,
};

VALUE create_native_schema_wrapper(VALUE rb_avro_schema) {
  VALUE rb_avro_schema_json = rb_funcall(rb_avro_schema, rb_intern("to_s"), 0);
  avro::ValidSchema* schema;
  try {
    avro::ValidSchema source_schema = avro::compileJsonSchemaFromString(StringValueCStr(rb_avro_schema_json));
    schema = new avro::ValidSchema(source_schema);
  } catch (const avro::Exception &e) {
    // This should never happen since the schema was valid in Ruby Avro
    rb_raise(rb_eRuntimeError, "Failed to compile native Avro schema '%s': %s", StringValueCStr(rb_avro_schema_json), e.what());
    // Make the compiler happy
    schema = NULL;
  }

  // Wrap the C++ object so it can be garbage collected by Ruby
  return TypedData_Wrap_Struct(RBASIC_CLASS(rb_avro_schema), &native_schema_type, schema);
}

const avro::ValidSchema* get_cached_schema(VALUE rb_avro_schema) {
  VALUE native_schema_wrapper = rb_iv_get(rb_avro_schema, "@native_schema");
  if (native_schema_wrapper == Qnil) {
    std::cout << "Schema cache miss for Avro schema " << rb_avro_schema << std::endl;
    native_schema_wrapper = create_native_schema_wrapper(rb_avro_schema);
    rb_iv_set(rb_avro_schema, "@native_schema", native_schema_wrapper);
  }

  avro::ValidSchema* schema;
  TypedData_Get_Struct(native_schema_wrapper, avro::ValidSchema, &native_schema_type, schema);
  return schema;
}

#include <ruby.h>
#include <encoder.hpp>
#include <decoder.hpp>

extern "C"
void Init_avromatic() {
  VALUE avromatic = rb_define_module("Avromatic");
  VALUE avromatic_io = rb_define_module_under(avromatic, "IO");
  VALUE avromatic_native = rb_define_module_under(avromatic_io, "Native");

  VALUE date_class = rb_const_get(rb_cObject, rb_intern("Date"));
  VALUE rb_epoch_start = rb_funcall(date_class, rb_intern("new"), 3, INT2NUM(1970), INT2NUM(1), INT2NUM(1));
  rb_const_set(avromatic_native, rb_intern("EPOCH_START"), rb_epoch_start);

  init_avromatic_encoder(avromatic_native);
  init_avromatic_decoder(avromatic_native);
}

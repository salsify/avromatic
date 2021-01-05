# frozen_string_literal: true

require 'mkmf'

have_library 'avrocpp'
append_cppflags ['-std=c++11', '-Wno-c++11-extensions']
create_makefile 'avromatic/avromatic'

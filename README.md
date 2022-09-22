# Avromatic

[![Build Status](https://circleci.com/gh/salsify/avromatic.svg?style=svg)][circleci]
[![Gem Version](https://badge.fury.io/rb/avromatic.svg)](https://badge.fury.io/rb/avromatic)

[circleci]: https://circleci.com/gh/salsify/avromatic

`Avromatic` generates Ruby models from [Avro](http://avro.apache.org/) schemas
and provides utilities to encode and decode them.

**This README reflects Avromatic 2.0. Please see the 
[1-0-stable](https://github.com/salsify/avromatic/blob/1-0-stable/README.md) branch for Avromatic 1.0.**

## Installation

Add this line to your application's Gemfile:

```ruby
gem 'avromatic'
```

And then execute:

    $ bundle

Or install it yourself as:

    $ gem install avromatic

See the [Logical Types](#logical-types) section below for details on using
Avromatic with unreleased Avro features.

## Usage

### Configuration

`Avromatic` supports the following configuration:

#### Model Generation

* **schema_store**: A schema store is required to load Avro schemas from the filesystem.
  It should be an object that responds to `find(name, namespace = nil)` and
  returns an `Avro::Schema` object. An `AvroTurf::SchemaStore` can be used.
  The `schema_store` is unnecessary if models are generated directly from 
  `Avro::Schema` objects. See [Models](#models).
* **nested_models**: An optional [ModelRegistry](https://github.com/salsify/avromatic/blob/master/lib/avromatic/model_registry.rb)
  that is used to store, by full schema name, the generated models that are
  embedded within top-level models. By default a new `Avromatic::ModelRegistry`
  is created.
* **eager_load_models**: An optional array of models, or strings with class
  names for models, that are added to `nested_models` at the end of
  `Avromatic.configure` and during code reloading in Rails applications. This
  option is useful for defining models that will be extended when the load order
  is important.
* **allow_unknown_attributes**: Optionally allow model constructors to silently
  ignore unknown attributes. Defaults to `false`. WARNING: Setting this to `true` 
  will result in incorrect union member coercions if an earlier union member is 
  satisfied by a subset of the latter union member's attributes.

#### Custom Types

See the section below on configuring [Custom Types](#custom-type-configuration).

#### Using a Schema Registry/Messaging API
 
The configuration options below are required when using a schema registry 
(see [Confluent Schema Registry](http://docs.confluent.io/2.0.1/schema-registry/docs/intro.html))
and the [Messaging API](#messaging-api).
  
* **schema_registry**: An `AvroSchemaRegistry::Client` or
  `AvroTurf::ConfluentSchemaRegistry` object used to store Avro schemas
  so that they can be referenced by id. Either `schema_registry` or
  `registry_url` must be configured.
* **registry_url**: URL for the schema registry. Either `schema_registry` or 
  `registry_url` must be configured.  The `build_schema_registry!` method may 
  be used to create a caching schema registry client instance based on other 
  configuration values.
* **use_schema_fingerprint_lookup**: Avromatic supports a Schema Registry
  [extension](https://github.com/salsify/avro-schema-registry#extensions) that
  provides an endpoint to lookup existing schema ids by fingerprint.
  A successful response from this GET request can be cached indefinitely.
  The use of this additional endpoint can be disabled by setting this option to
  `false` and this is recommended if using a Schema Registry that does not support
  the endpoint.
* **messaging**: An `AvroTurf::Messaging` object to be shared by all generated models
  The `build_messaging!` method may be used to create a `Avromatic::Messaging`
  instance based on the other configuration values.
* **logger**: The logger to use for the schema registry client.

Example using a schema registry:

```ruby
Avromatic.configure do |config|
  config.schema_store = AvroTurf::SchemaStore.new(path: 'avro/schemas')
  config.registry_url = Rails.configuration.x.avro_schema_registry_url
  config.build_schema_registry!
  config.build_messaging!
end
```

#### Decoding

* **use_custom_datum_reader**: `Avromatic` includes a modified subclass of
  `Avro::IO::DatumReader`. This subclass returns additional information about
  the index of union members when decoding Avro messages. This information is
  used to optimize model creation when decoding. By default this information
  is included in the hash returned by the `DatumReader` but can be omitted by
  setting this option to `false`.

#### Encoding
* **use_custom_datum_writer**: `Avromatic` includes a modified subclass of
  `Avro::IO::DatumWriter`. This subclass supports caching avro encodings for 
  immutable models and uses additional information about the index of union 
  members to optimize the encoding of Avro messages. By default this 
  information is included in the hash passed to the encoder but can be omitted
  by setting this option to `false`.


### Models

Models are defined based on an Avro schema for a record.

The Avro schema can be specified by name and loaded using the schema store:

```ruby
class MyModel
  include Avromatic::Model.build(schema_name: :my_model)
end

# Construct instances by passing in a hash of attributes
instance = MyModel.new(id: 123, name: 'Tesla Model 3', enabled: true)

# Access attribute values with readers
instance.name # => "Tesla Model 3"

# Models are immutable by default
instance.name = 'Tesla Model X' # => NoMethodError (private method `name=' called for #<MyModel:0x00007ff711e64e60>) 

# Booleans can also be accessed by '?' readers that coerce nil to false
instance.enabled? # => true

# Models implement ===, eql? and hash
instance == MyModel.new(id: 123, name: 'Tesla Model 3', enabled: true) # => true
instance.eql?(MyModel.new(id: 123, name: 'Tesla Model 3', enabled: true)) # => true
instance.hash # => -1279155042741869898

# Retrieve a hash of the model's attributes via to_h, to_hash or attributes
instance.to_h # => {:id=>123, :name=>"Tesla Model 3", :enabled=>true}
```

Or an `Avro::Schema` object can be specified directly:

```ruby
class MyModel
  include Avromatic::Model.build(schema: schema_object)
end
```

A specific subject name can be associated with the schema:
```ruby
class MyModel
  include Avromatic::Model.build(schema_name: 'my_model',
                                 schema_subject: 'my_model-value')
end
```

Models are generated as immutable value
objects by default, but can optionally be defined as mutable:

```ruby
class MyModel
  include Avromatic::Model.build(schema_name: :my_model, mutable: true)
end
```

Generated models include attributes for each field in the Avro schema
including any default values defined in the schema.

A model may be defined with both a key and a value schema:

```ruby
class MyTopic
  include Avromatic::Model.build(value_schema_name: :topic_value,
                                 key_schema_name: :topic_key)
end
```

When key and value schemas are both specified, attributes are added to the model
for the union of the fields in the two schemas.

By default, optional fields are not allowed in key schemas since their values may
be accidentally omitted leading to problems if data is partitioned based on the
key values.

This behavior can be overridden by specifying the `:allow_optional_key_fields`
option for the model:

```ruby
class MyTopic
  include Avromatic::Model.build(value_schema_name: :topic_value,
                                 key_schema_name: :topic_key,
                                 allow_optional_key_fields: true)
end
```

A specific subject name can be associated with both the value and key schemas:
```ruby
class MyTopic
  include Avromatic::Model.build(value_schema_name: :topic_value,
                                 value_schema_subject: 'topic_value-value',
                                 key_schema_name: :topic_key,
                                 key_schema_subject: 'topic_key-value')
end
```

A model can also be generated as an anonymous class that can be assigned to a
constant:

```ruby
MyModel = Avromatic::Model.model(schema_name :my_model)
```

#### Experimental: Union Support

Avromatic contains experimental support for unions containing more than one
non-null member type. This feature is experimental because Avromatic
may attempt to coerce between types too aggressively.

For now, if a union contains [nested models](#nested-models) then it is
recommended that you assign model instances.

Some combination of the ordering of member types in the union and relying on
model validation may be required so that the correct member is selected,
especially when deserializing from Avro.

In the future, the type coercion used in the gem will be enhanced to better
support the union use case.

#### Nested Models

Nested models are models that are embedded within top-level models generated
using Avromatic. Normally these nested models are automatically generated.

By default, nested models are stored in `Avromatic.nested_models`. This is an
`Avromatic::ModelRegistry` instance that provides access to previously generated
nested models by the full name of their Avro schema.

```ruby
Avromatic.nested_models['com.my_company.test.example']
#=> <model class>
```

The `ModelRegistry` can be customized to remove a namespace prefix:

```ruby
Avromatic.nested_models =
  Avromatic::ModelRegistry.new(remove_namespace_prefix: 'com.my_company')
```

The `:remove_namespace_prefix` value can be a string or a regexp.

By default, top-level generated models reuse `Avromatic.nested_models`. This
allows nested models to be shared across different generated models.
A `:nested_models` option can be specified when generating a model. This allows
the reuse of nested models to be scoped:

```ruby
Avromatic::Model.model(schema_name, :my_model
                       nested_models: ModelRegistry.new)
```

Only models without a key schema can be used as nested models. When a model is
generated with just a value schema then it is automatically registered so that
it can be used as a nested model.

To extend a model that will be used as a nested model, you must ensure that it
is defined, which will register it, prior it being referenced by another model.

Using the `Avromatic.eager_load_models` option allows models that are extended
and will be used as nested models to be defined at the end of the `.configure`
block. In Rails applications, these models are also re-registered after
`nested_models` is cleared when code reloads to ensure that classes load in the
correct order:

```ruby
Avromatic.configure do |config|
  config.eager_load_models = [
    # reference any extended models that should be defined first
    'MyNestedModel'
  ]
end
```

#### Custom Type Configuration

Custom types can be configured for fields of named types (record, enum, fixed).
These customizations are registered on the `Avromatic` module. Once a custom type
is registered, it is used for all models with a schema that references that type.
It is recommended to register types within a block passed to `Avromatic.configure`:

```ruby
Avromatic.configure do |config|
  config.register_type('com.example.my_string', MyString)
end
```

The full name of the type and an optional class may be specified. When a class is
provided then values for attributes of that type are defined using the specified 
class.

If the provided class responds to the class methods `from_avro` and `to_avro`
then those methods are used to convert values when assigning to the model and 
before encoding using Avro respectively.

`from_avro` and `to_avro` methods may be also be specified as Procs when 
registering the type:

```ruby
Avromatic.configure do |config|
  config.register_type('com.example.updown_string') do |type|
    type.from_avro = ->(value) { value.upcase }
    type.to_avro = ->(value) { value.downcase }
  end
end
```

Nil handling is not required as the conversion methods are not be called if the
inbound or outbound value is nil.

If a custom type is registered for a record-type field, then any `to_avro` 
method/Proc should return a Hash with string keys for encoding using Avro.

### Encoding and Decoding

`Avromatic` provides two different interfaces for encoding the key (optional)
and value associated with a model.

#### Manually Managed Schemas

The attributes for the value schema used to define a model can be encoded using:

```ruby
encoded_value = model.avro_raw_value
```

In order to decode this data, a copy of the value schema is required.

If a model also has an Avro schema for a key, then the key attributes can be
encoded using:

```ruby
encoded_key = model.avro_raw_key
```

If attributes were encoded using the same schema(s) used to define a model, then
the data can be decoded to create a new model instance:

```ruby
MyModel.avro_raw_decode(key: encoded_key, value: encoded_value)
```

If the attributes where encoded using a different version of the model's schemas,
then a new model instance can be created by also providing the schemas used to 
encode the data:

```ruby
MyModel.avro_raw_decode(key: encoded_key,
                        key_schema: writers_key_schema,
                        value: encoded_value,
                        value_schema: writers_value_schema)
```

#### Messaging API

The other interface for encoding and decoding attributes uses the
`AvroTurf::Messaging` API. This interface leverages a schema registry and
prefixes the encoded data with an id to identify the schema. In this approach,
a schema registry is used to ensure that the correct schemas are available during
decoding.

The attributes for the value schema can be encoded with a schema id prefix using:

```ruby
message_value = model.avro_message_value
```

If a model has an Avro schema for a key, then those attributes can also be encoded
prefixed with a schema id:

```ruby
message_key = model.avro_message_key
```

A model instance can be created from a key and value encoded in this manner:

```ruby
MyTopic.avro_message_decode(message_key, message_value)
```

Or just a value if only one schema is used:

```ruby
MyValue.avro_message_decode(message_value)
```

The schemas associated with a model can also be added to a schema registry without
encoding a message:

```ruby
MyTopic.register_schemas!
```

#### Avromatic::Model::MessageDecoder

A stream of messages encoded from various models using the messaging approach
can be decoded using `Avromatic::Model::MessageDecoder`. The decoder must be 
initialized with the list of models to decode:

```ruby
decoder = Avromatic::Model::MessageDecoder.new(MyModel1, MyModel2)

decoder.decode(model1_messge_key, model1_message_value)
# => instance of MyModel1
decoder.decode(model2_message_value)
# => instance of MyModel2
```

### Validations and Coercions

An exception will be thrown if an attribute value cannot be coerced to the corresponding Avro schema field's type.
The following coercions are supported:

| Ruby Type | Avro Type |
| --------- | --------- |
| String, Symbol | string |
| Array | array |
| Hash | map |
| Integer, Float | int |
| Integer | long |
| Float | float |
| Float | double |
| String | bytes |
| Date, Time, DateTime | date |
| Time, DateTime | timestamp-millis |
| Time, DateTime | timestamp-micros |
| TrueClass, FalseClass | boolean |
| NilClass | null |
| Hash | record |

Validation of required fields is done automatically when serializing a model to Avro. It can also be done
explicitly by calling the `valid?` or `invalid?` methods from the 
[ActiveModel::Validations](https://edgeapi.rubyonrails.org/classes/ActiveModel/Validations.html) interface.

### RSpec Support

This gem also includes an `"avromatic/rspec"` file that can be required to support
using Avromatic with a fake schema registry during tests.

Requiring this file configures a RSpec before hook that directs any schema
registry requests to a fake, in-memory schema registry and rebuilds the
`Avromatic::Messaging` object for each example.

## Development

After checking out the repo, run `bin/setup` to install dependencies. Then, run `rake spec` to run the tests. You can also run `bin/console` for an interactive prompt that will allow you to experiment.

To install this gem onto your local machine, run `bundle exec rake install`. To release a new version, update the version number in `version.rb`, and then run `bundle exec rake release`, which will create a git tag for the version, push git commits and tags, and push the `.gem` file to [rubygems.org](https://rubygems.org).

## Contributing

Bug reports and pull requests are welcome on GitHub at https://github.com/salsify/avromatic.


## License

The gem is available as open source under the terms of the [MIT License](http://opensource.org/licenses/MIT).


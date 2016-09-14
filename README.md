# Avromatic

[![Build Status](https://travis-ci.org/salsify/avromatic.svg?branch=master)][travis]
[![Gem Version](https://badge.fury.io/rb/avromatic.svg)](https://badge.fury.io/rb/avromatic)

[travis]: http://travis-ci.org/salsify/avromatic

`Avromatic` generates Ruby models from [Avro](http://avro.apache.org/) schemas
and provides utilities to encode and decode them.

## Installation

Add this line to your application's Gemfile:

```ruby
gem 'avromatic'
```

And then execute:

    $ bundle

Or install it yourself as:

    $ gem install avromatic

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

#### Using a Schema Registry/Messaging API
 
The configuration options below are required when using a schema registry 
(see [Confluent Schema Registry](http://docs.confluent.io/2.0.1/schema-registry/docs/intro.html))
and the [Messaging API](#messaging-api).
  
* **schema_registry**: An `AvroTurf::SchemaRegistry` object used to store Avro schemas 
  so that they can be referenced by id. Either `schema_registry` or 
  `registry_url` must be configured.
* **registry_url**: URL for the schema registry. Either `schema_registry` or 
  `registry_url` must be configured.
* **messaging**: An `AvroTurf::Messaging` object to be shared by all generated models.
  The `build_messaging!` method may be used to create a `Messaging` instance based
  on the other configuration values.
* **logger**: The logger to use for the schema registry client.
* [Custom Types](#custom-types)

Example using a schema registry:

```ruby
Avromatic.configure do |config|
  config.schema_store = AvroTurf::SchemaStore.new(path: 'avro/schemas')
  config.registry_url = Rails.configuration.x.avro_schema_registry_url
  config.build_messaging!
end
```

### Models

Models are defined based on an Avro schema for a record.

The Avro schema can be specified by name and loaded using the schema store:

```ruby
class MyModel
  include Avromatic::Model.build(schema_name :my_model)
end
```

Or an `Avro::Schema` object can be specified directly:

```ruby
class MyModel
  include Avromatic::Model.build(schema: schema_object)
end
```

Models are generated as [Virtus](https://github.com/solnic/virtus) value
objects. `Virtus` attributes are added for each field in the Avro schema
including any default values defined in the schema. `ActiveModel` validations
are used to define validations on certain types of fields ([see below](#validations)).

A model may be defined with both a key and a value schema:

```ruby
class MyTopic
  include Avromatic::Model.build(value_schema_name: :topic_value,
                                 key_schema_name: :topic_key)
end
```

When key and value schemas are both specified, attributes are added to the model
for the union of the fields in the two schemas.

A model can also be generated as an anonymous class that can be assigned to a
constant:

```ruby
MyModel = Avromatic::Model.model(schema_name :my_model)
```

#### Experimental: Union Support

Avromatic contains experimental support for unions containing more than one
non-null member type. This feature is experimental because Virtus attributes
may attempt to coerce between types too aggressively.

In the future, the type coercion used in the gem will be replaced to better
support the union use case.

For now, if a union contains nested models then it is recommended that you
assign instances

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
  Avromatic::ModelRegistry.new(remove_namespace_prefix: 'com.my_company'
```

By default, top-level generated models reuse `Avromatic.nested_models`. This
allows nested models to be shared across different generated models.
A `:nested_models` option can be specified when generating a model. This allows
the reuse of nested models to be scoped:

```ruby
Avromatic::Model.model(schema_name, :my_model
                       nested_models: ModelRegistry.new)
```

It is also possible to explicitly generate a nested model that should be reused
and add it to the registry. This is useful when the nested model is extended:

```ruby
class UsefulSubrecord
  include Avromatic::Model.build(schema_name: 'useful_subrecord')

  def do_something_custom
    ...
  end
end
Avromatic.nested_models.register(UsefulSubrecord)
```

#### Custom Types

Custom types can be configured for fields of named types (record, enum, fixed).
These customizations are registered on the `Avromatic` module. Once a custom type
is registered, it is used for all models with a schema that references that type.
It is recommended to register types within a block passed to `Avromatic.configure`:

Note: custom types are not currently supported on members of unions with more
than one non-null type.

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

### Validations

The following validations are supported:

- The size of the value for a fixed type field.
- The value for an enum type field is in the declared set of values.
- Presence of a value for required fields.

### Unsupported/Future

The following types/features are not supported for generated models:

- Custom types for members within a union.
- Reused models for nested records: Currently an anonymous model class is
  generated for each subrecord.

## Development

After checking out the repo, run `bin/setup` to install dependencies. Then, run `rake spec` to run the tests. You can also run `bin/console` for an interactive prompt that will allow you to experiment.

To install this gem onto your local machine, run `bundle exec rake install`. To release a new version, update the version number in `version.rb`, and then run `bundle exec rake release`, which will create a git tag for the version, push git commits and tags, and push the `.gem` file to [rubygems.org](https://rubygems.org).

## Contributing

Bug reports and pull requests are welcome on GitHub at https://github.com/salsify/avromatic.


## License

The gem is available as open source under the terms of the [MIT License](http://opensource.org/licenses/MIT).


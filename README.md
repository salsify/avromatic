# Avromatic

[![Build Status](https://travis-ci.org/salsify/avromatic.svg?branch=master)][travis]

[travis]: http://travis-ci.org/salsify/avromatic

`Avromatic` generates Ruby models from Avro schemas and provides utilities to
encode and decode them.

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

* registry_url: URL for the schema registry. The schema registry is used to store
  Avro schemas so that they can be referenced by id.
* schema_store: The schema store is used to load Avro schemas from the filesystem.
  It should be an object that responds to `find(name, namespace = nil)` and
  returns an `Avro::Schema` object.
* messaging: An `AvroTurf::Messaging` object to be shared by all generated models.
  The `build_messaging!` method may be used to create a `Messaging` instance based
  on the other configuration values.
* logger: The logger is for the schema registry client.

### Models

Models may be defined based on an Avro schema for a record.

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
are used to define validations on certain types of fields.

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

#### Encode/Decode

Models can be encoded using Avro leveraging a schema registry to encode a schema
id at the beginning of the value.

```ruby
model.avro_message_value
```

If a model has a Avro schema for a key, then the key can also be encoded
prefixed with a schema id.

```ruby
model.avro_message_key
```

A model instance can be created from an Avro-encoded value and an Avro-encoded
optional key:

```ruby
MyTopic.deserialize(message_key, message_value)
```

Or just a value if only one schema is used:

```ruby
MyValue.deserialize(message_value)
```

#### Decoder

A stream of messages encoded from various models can be deserialized using
`Avromatic::Model::Decoder`. The decoder must be initialized with the list
of models to decode:

```ruby
decoder = Avromatic::Model::Decoder.new(MyModel1, MyModel2)

decoder.decode(model1_key, model1_value)
# => instance of MyModel1
decoder.decode(model2_value)
# => instance of MyModel2
```

#### Validations

The following validations are supported:

- The size of the value for a fixed type field.
- The value for an enum type field is in the declared set of values.
- Presence of a value for required fields.

#### Unsupported/Future

The following types/features are not supported for generated models:

- Generic union fields: The special case of an optional field, the union of `:null` and
  another type, is supported.
- Reused models for nested records: Currently an anonymous model class is
  generated for each subrecord.

## Development

After checking out the repo, run `bin/setup` to install dependencies. Then, run `rake spec` to run the tests. You can also run `bin/console` for an interactive prompt that will allow you to experiment.

To install this gem onto your local machine, run `bundle exec rake install`. To release a new version, update the version number in `version.rb`, and then run `bundle exec rake release`, which will create a git tag for the version, push git commits and tags, and push the `.gem` file to [rubygems.org](https://rubygems.org).

## Contributing

Bug reports and pull requests are welcome on GitHub at https://github.com/salsify/avromatic.


## License

The gem is available as open source under the terms of the [MIT License](http://opensource.org/licenses/MIT).


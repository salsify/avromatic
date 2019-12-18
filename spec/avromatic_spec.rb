# frozen_string_literal: true

describe Avromatic do
  it "has a version number" do
    expect(Avromatic::VERSION).not_to be_nil
  end

  describe ".build_schema_registry" do
    context "when the registry_url is unset" do
      before { Avromatic.registry_url = nil }

      it "raises an error" do
        expect { Avromatic.build_schema_registry }
          .to raise_error('Avromatic must be configured with a registry_url')
      end
    end

    context "when the registry_url is set" do
      let(:registry_url) { 'http://registry.example.com' }

      before do
        Avromatic.registry_url = registry_url
      end

      it "returns an AvroSchemaRegistry::CachedClient", :aggregate_failures do
        allow(AvroSchemaRegistry::Client).to receive(:new).and_call_original
        expect(Avromatic.build_schema_registry).to be_a(AvroSchemaRegistry::CachedClient)
        expect(AvroSchemaRegistry::Client).to have_received(:new)
          .with(Avromatic.registry_url, logger: Avromatic.logger)
      end

      context "when use_schema_fingerprint_lookup is false" do
        before { Avromatic.use_schema_fingerprint_lookup = false }

        it "returns a CachedConfluentSchemaRegistry client", :aggregate_failures do
          allow(AvroTurf::ConfluentSchemaRegistry).to receive(:new).and_call_original
          schema_registry = Avromatic.build_schema_registry
          expect(schema_registry).to be_a(AvroTurf::CachedConfluentSchemaRegistry)
          expect(schema_registry).not_to be_a(AvroSchemaRegistry::CachedClient)
          expect(AvroTurf::ConfluentSchemaRegistry).to have_received(:new)
            .with(Avromatic.registry_url, logger: Avromatic.logger)
        end
      end

      it "does not cache the schema registry client" do
        expect(Avromatic.build_schema_registry).not_to equal(Avromatic.build_schema_registry)
      end
    end
  end

  context "eager loading models" do
    before do
      stub_const('NestedRecord', Avromatic::Model.model(schema_name: 'test.nested_record'))
      described_class.nested_models.clear
    end

    context "at the end of configure" do
      it "registers models" do
        described_class.configure do |config|
          config.eager_load_models = NestedRecord
        end
        expect(described_class.nested_models.registered?('test.nested_record')).to be(true)
      end
    end

    describe "#prepare!" do
      before do
        stub_const('ValueModel', Avromatic::Model.model(schema_name: 'test.value'))
        allow(Avromatic.schema_store).to receive(:clear)
      end

      it "clears the registry" do
        described_class.prepare!
        expect(described_class.nested_models.registered?('test.value')).to be(false)
      end

      it "clears the schema store" do
        described_class.prepare!
        expect(Avromatic.schema_store).to have_received(:clear)
      end

      context "when skip_clear is true" do
        it "does not clear the registry" do
          described_class.prepare!(skip_clear: true)
          expect(described_class.nested_models.registered?('test.value')).to be(true)
        end

        it "does not clear the schema store" do
          described_class.prepare!(skip_clear: true)
          expect(Avromatic.schema_store).not_to have_received(:clear)
        end

        it "doesn't registers nested models" do
          described_class.eager_load_models = %w(NestedRecord)
          described_class.prepare!(skip_clear: true)
          expect(described_class.nested_models.registered?('test.__nested_record_sub_record')).to be(false)
        end
      end

      it "registers models" do
        described_class.eager_load_models = %w(NestedRecord)
        described_class.prepare!
        expect(described_class.nested_models.registered?('test.value')).to be(false)
      end

      it "registers nested models" do
        described_class.eager_load_models = %w(NestedRecord)
        described_class.prepare!
        expect(described_class.nested_models.registered?('test.__nested_record_sub_record')).to be(true)
      end
    end
  end
end

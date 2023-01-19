package com.ipt.kafkatopicupdates.streams;

import org.apache.kafka.streams.kstream.ValueTransformerWithKey;
import org.apache.kafka.streams.kstream.ValueTransformerWithKeySupplier;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;

public class DeduplicationStringTransformerSupplier implements ValueTransformerWithKeySupplier<String, String, String> {

    private final String storeName;

    public DeduplicationStringTransformerSupplier(String storeName) {
        this.storeName = storeName;
    }
    @Override
    public ValueTransformerWithKey<String, String, String> get() {
        return new ValueTransformerWithKey<>() {
            private KeyValueStore<String, String> state;

            @Override
            public void init(ProcessorContext context) {
                state = context.getStateStore(storeName);
            }

            @Override
            public String transform(final String key, final String value) {
                String prevValue = state.get(key);
                if (prevValue != null && prevValue.equals(value)) {
                    return null;
                }
                state.put(key, value);
                return value;
            }

            @Override
            public void close() { /* TODO document why this method is empty */ }
        };
    }
}

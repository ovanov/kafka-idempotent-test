package com.ipt.kafkatopicupdates.streams;

import org.apache.kafka.streams.kstream.ValueTransformerWithKey;
import org.apache.kafka.streams.kstream.ValueTransformerWithKeySupplier;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;

public class DeduplicationTransformerSupplier implements ValueTransformerWithKeySupplier<String, String, String> {

    private final String storeName;

    public DeduplicationTransformerSupplier(String storeName) {
        this.storeName = storeName;
    }
    @Override
    public ValueTransformerWithKey<String, String, String> get() {
        return new ValueTransformerWithKey() {
            private KeyValueStore<String, String> state;

            @Override
            public void init(ProcessorContext context) {
                state = context.getStateStore(storeName);
            }

            @Override
            public Object transform(Object readOnlyKey, Object value) {
                String stringKey = (String) readOnlyKey;
                String stringValue = (String) value;
                String prevValue = state.get(stringKey);
                if (prevValue != null && prevValue.equals(value)) {
                    return null;
                }
                state.put(stringKey, stringValue);
                return value;
            }

            @Override
            public void close() { /* TODO document why this method is empty */ }
        };
    }
}

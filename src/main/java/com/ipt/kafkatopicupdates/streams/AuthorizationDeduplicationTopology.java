package com.ipt.kafkatopicupdates.streams;

import ch.ipt.kafka.avro.Authorization;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.stereotype.Component;

@Component
public class AuthorizationDeduplicationTopology {

    private static final Logger LOGGER = LoggerFactory.getLogger(AuthorizationDeduplicationTopology.class);

    public void buildTopology(StreamsBuilder streamsBuilder, String sourceTopic, String sinkTopic, String storeName) {
        StoreBuilder<KeyValueStore<String, String>> keyValueStoreStoreBuilder =
                Stores.keyValueStoreBuilder(
                        Stores.persistentKeyValueStore(storeName),
                        Serdes.String(),
                        Serdes.String());

        streamsBuilder.addStateStore(keyValueStoreStoreBuilder);

        KStream<String, Authorization> stream = streamsBuilder.stream(sourceTopic);

        stream
                .mapValues(
                        (readOnlyKey, value) -> String.valueOf(value.getAuthorized())
                )
                .transformValues(
                        new DeduplicationTransformerSupplier(storeName),
                        storeName
                )
                .filter(
                        (key, value) -> value != null
                )
                .mapValues(
                        (readOnlyKey, value) -> new Authorization(readOnlyKey, value.equals("true"))
                )
                .peek((key, value) -> LOGGER.info("Message: key={}, value={}", key, value))
                .to(sinkTopic);
    }

}
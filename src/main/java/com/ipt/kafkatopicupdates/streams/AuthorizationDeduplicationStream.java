package com.ipt.kafkatopicupdates.streams;

import ch.ipt.kafka.avro.Authorization;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.stereotype.Component;

@Component
public class AuthorizationDeduplicationStream {

    @Value("${topics.authorizations-with-duplicates}")
    private String sourceTopic;
    @Value("${topics.authorizations-without-duplicates}")
    private String sinkTopic;

    private static final String STORE_NAME = "authorization-store-3";

    private static final Logger LOGGER = LoggerFactory.getLogger(AuthorizationDeduplicationStream.class);

    @Bean
    public NewTopic topicExampleFiltered() {
        return TopicBuilder.name(sinkTopic)
                .partitions(3)
                .replicas(3)
                .build();
    }

    @Autowired
    void buildPipeline(StreamsBuilder streamsBuilder) {

        StoreBuilder<KeyValueStore<String, String>> keyValueStoreStoreBuilder =
                Stores.keyValueStoreBuilder(
                        Stores.persistentKeyValueStore(STORE_NAME),
                        Serdes.String(),
                        Serdes.String());

        streamsBuilder.addStateStore(keyValueStoreStoreBuilder);

        KStream<String, Authorization> stream = streamsBuilder.stream(sourceTopic);

        stream
                .mapValues(
                        (readOnlyKey, value) -> String.valueOf(value.getAuthorized())
                )
                .transformValues(
                        new DeduplicationStringTransformerSupplier(STORE_NAME),
                        STORE_NAME
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
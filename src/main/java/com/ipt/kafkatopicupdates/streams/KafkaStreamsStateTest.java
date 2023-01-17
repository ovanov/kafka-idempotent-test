package com.ipt.kafkatopicupdates.streams;


import ch.ipt.kafka.avro.Authorization;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.Map;


@Component
public class KafkaStreamsStateTest {

    @Value("${source-topic-authorizations}")
    private String sourceTopic;
    String sinkTopic = "state-transfer";

    private static final Serde<Authorization> AUTHORIZATION_SERDE = new SpecificAvroSerde<>();

    private static final String STORE_NAME = "authorization-store";

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaStreamsStateTest.class);

    @Bean
    public NewTopic topicExampleFiltered() {
        return TopicBuilder.name(sinkTopic)
                .partitions(3)
                .replicas(3)
                .build();
    }

    @Autowired
    void buildPipeline(StreamsBuilder streamsBuilder) {

        StoreBuilder<KeyValueStore<String, Authorization>> keyValueStoreStoreBuilder =
                Stores.keyValueStoreBuilder(
                        Stores.persistentKeyValueStore(STORE_NAME),
                        Serdes.String(),
                        Serdes.serdeFrom(AUTHORIZATION_SERDE.serializer(), AUTHORIZATION_SERDE.deserializer())
                );

        streamsBuilder.addStateStore(keyValueStoreStoreBuilder);

        KStream<String, Authorization> stream = streamsBuilder.stream(sourceTopic);

        stream
                .transformValues(() -> new ValueTransformerWithKey<String, Authorization, Authorization>() {
                    private KeyValueStore<String, Authorization> state;
                    @Override
                    public void init(ProcessorContext context) {
                        state = context.getStateStore(STORE_NAME);
                    }
                    @Override
                    public Authorization transform(final String key, final Authorization value) {
                        Authorization prevValue = state.get(key);
                        if (prevValue.getAuthorized() == value.getAuthorized()) {
                            return null;
                        }
                        state.put(key, value);
                        return value;
                    }

                    @Override
                    public void close() {}
                }, STORE_NAME)
                .filter(
                        (key, value) -> value != null
                )
                .peek((key, value) -> LOGGER.info("Message: key={}, value={}", key, value))
                .to(sinkTopic);
    }

}
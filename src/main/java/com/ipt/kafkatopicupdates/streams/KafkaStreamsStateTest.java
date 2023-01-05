package com.ipt.kafkatopicupdates.streams;


import ch.ipt.kafka.avro.Authorization;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.stereotype.Component;


@Component
public class KafkaStreamsStateTest {

    @Value("${source-topic-authorizations}")
    private String sourceTopic;
    String sinkTopic = "state-transfer";

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
        KStream<String, Authorization> stream = streamsBuilder.stream(sourceTopic);
        stream
                .filter((key, authorization) -> !authorization.getAuthorized())
                .peek((key, authorization) -> LOGGER.info("Message: key={}, value={}", key, authorization));
        stream.to(sinkTopic);

    }

}
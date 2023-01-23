package com.ipt.kafkatopicupdates.streams;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.streams.StreamsBuilder;

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

    @Bean
    public NewTopic topicExampleFiltered() {
        return TopicBuilder.name(sinkTopic)
                .partitions(3)
                .replicas(3)
                .build();
    }

    @Autowired
    public void buildPipeline(StreamsBuilder streamsBuilder, DefaultStreamsTopology defaultStreamsTopology) {

        defaultStreamsTopology.buildTopology(streamsBuilder, sourceTopic, sinkTopic, STORE_NAME);

    }
    

}
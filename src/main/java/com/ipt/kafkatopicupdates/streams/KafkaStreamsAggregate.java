package com.ipt.kafkatopicupdates.streams;


import ch.ipt.kafka.avro.Authorization;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;


@Component
public class KafkaStreamsAggregate {

    private String sourceTopic = "authorization";
    String sinkTopic = "state-transfer";

    private static final Serde<String> STRING_SERDE = Serdes.String();
    private static final Serde<Authorization> AUTHORIZATION_SERDE = new SpecificAvroSerde<>();




    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaStreamsAggregate.class);

    @Autowired
    void buildPipeline(StreamsBuilder streamsBuilder) {


        streamsBuilder.stream(sourceTopic, Consumed.with(STRING_SERDE, AUTHORIZATION_SERDE))
                .peek(((key, value) -> LOGGER.info("test")));
                /*.groupByKey()
                .aggregate(
                    () -> new Authorization(),
                    (key, value, aggregate) -> {
                        if (value.equals("true")) {
                            Materialized.as("state-store0");
                            return value;
                        }
                        return null;
                    }
                )
                .toStream()
                .to(sinkTopic, Produced.with(STRING_SERDE, AUTHORIZATION_SERDE));*/

        LOGGER.info(String.valueOf(streamsBuilder.build().describe()));
    }

}
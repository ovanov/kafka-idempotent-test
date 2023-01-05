package com.ipt.kafkatopicupdates.streams;


import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;

import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;

import org.apache.kafka.streams.state.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;


@Component
public class KafkaStreamsAggregate {

    private String sourceTopic = "account";
    String sinkTopic = "state-transfer";

    private static final Serde<String> STRING_SERDE = Serdes.String();



    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaStreamsAggregate.class);

    @Autowired
    void buildPipeline(StreamsBuilder streamsBuilder) {


        KStream<String, String> stateStream = streamsBuilder.stream(sourceTopic, Consumed.with(STRING_SERDE, STRING_SERDE))
                .peek((key, value) -> LOGGER.info("Outgoing record - key " +key +" value " + value));

        KTable<String, String> stateTable = stateStream.groupByKey().aggregate(
                () -> null,
                (key, value, aggregate) -> {
                    if (value.equals("true")) {
                        Materialized.as("state-store0");
                        return value;
                    }
                    return null;
                }
        );

        KStream<String, String> updated3ds = stateTable.toStream();

        updated3ds.to(sinkTopic, Produced.with(STRING_SERDE, STRING_SERDE));




        // ksql
//        CREATE stream ACCOUNTS WITH (KAFKA_TOPIC='accounts', VALUE_FORMAT='avro', partitions=1);
//
//        CREATE STREAM TRANSACTIONSREKEYED
//        WITH (PARTITIONS=1) AS
//        SELECT *
//                FROM TRANSACTIONS
//        PARTITION BY accountId;
//
//        CREATE TABLE TOTALTRANSACTIONS AS
//        SELECT a.accountId, SUM(t.amount) AS sum_all_transactions
//        FROM TRANSACTIONSREKEYED t LEFT OUTER JOIN ACCOUNTS a
//        WITHIN 7 DAYS
//        ON t.accountId = a.accountId
//        GROUP BY a.accountId
//        EMIT CHANGES;

        LOGGER.info(String.valueOf(streamsBuilder.build().describe()));
    }

}
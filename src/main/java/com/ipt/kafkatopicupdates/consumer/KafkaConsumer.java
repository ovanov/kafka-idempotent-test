package com.ipt.kafkatopicupdates.consumer;


import ch.ipt.kafka.avro.Authorization;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;


//This class is only needed when we want to consume messages directly
@Component
public class KafkaConsumer {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaConsumer.class);
    @KafkaListener(id = "authorization-consumer",
            topics = "authorization")
    public void receiveNewState(ConsumerRecord<String, Authorization> consumerRecord) {
        String key = consumerRecord.key();
        Authorization value = consumerRecord.value();
        LOGGER.info("Received: key={}, value={}", key, value);
    }
}

package com.ipt.kafkatopicupdates.consumer;


import ch.ipt.kafka.avro.Account;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;


//This class is only needed when we want to consume messages directly
//@Component
public class KafkaConsumer {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaConsumer.class);

    /*@KafkaListener(id = "transactions-consumer",
            topics = "transactions")
    public void receiveTransactionMessage(ConsumerRecord<String, Payment> consumerRecord) {
        String key = consumerRecord.key();
        Payment value = consumerRecord.value();
        LOGGER.info("received credit message: key={}, value={}", key, value);
    }*/

    @KafkaListener(id = "accounts-consumer",
            topics = "compacted")
    public void receiveAccountMessage(ConsumerRecord<String, String> consumerRecord) {
        String key = consumerRecord.key();
        String value = consumerRecord.value();
        LOGGER.info("received account message: key={}, value={}", key, value);
    }

    @KafkaListener(id = "state-transfer-consumer",
            topics = "state-transfer")
    public void receiveNewState(ConsumerRecord<String, String> consumerRecord) {
        String key = consumerRecord.key();
        String value = consumerRecord.value();
        LOGGER.info("received NEW 3DS STATE: key={}, value={}", key, value);
    }
}

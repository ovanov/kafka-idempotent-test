package com.ipt.kafkatopicupdates.producer;

import ch.ipt.kafka.avro.Authorization;
import com.google.rpc.context.AttributeContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

@Configuration
public class AuthorizationAvroProducer {

    private static final Logger LOGGER = LoggerFactory.getLogger(AuthorizationAvroProducer.class);

    private int counter = 0;

    @Value("${source-topic-authorizations}")
    private String sourceTopic;

    @Autowired
    KafkaTemplate<String, String> kafkaTemplatePayment;


    @Scheduled(fixedRate = 2000)
    private void scheduleFixedRateTask() {
        Authorization authorization = new Authorization("1", counter > 5);
        if(counter > 5) {
            counter = 0;
        } else {
            counter++;
        }

        ListenableFuture<SendResult<String, String>> future =
                kafkaTemplatePayment.send(
                        sourceTopic,
                        authorization.getAccountId().toString(),
                        (!authorization.getAuthorized()) ? "false" : "true"
                );

        future.addCallback(new ListenableFutureCallback<>() {

            @Override
            public void onSuccess(SendResult<String, String> result) {
                LOGGER.info("Message [{}] delivered with offset {}",
                        authorization,
                        result.getRecordMetadata().offset());
            }

            @Override
            public void onFailure(Throwable ex) {
                LOGGER.warn("Unable to deliver message [{}]. {}",
                        authorization,
                        ex.getMessage());
            }
        });
    }

}

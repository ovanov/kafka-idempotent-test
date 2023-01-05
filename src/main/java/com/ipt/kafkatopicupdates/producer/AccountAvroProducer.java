package com.ipt.kafkatopicupdates.producer;

import ch.ipt.kafka.avro.Account;
import org.apache.kafka.clients.admin.NewTopic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.kafka.support.serializer.JsonSerializer;

import javax.annotation.PostConstruct;
import java.util.Arrays;

@Configuration
public class AccountAvroProducer {

    private static final Logger LOGGER = LoggerFactory.getLogger(AccountAvroProducer.class);

    private String sourceTopic = "compacted";
    @Autowired
    KafkaTemplate<String, String> kafkaTemplateAccount;



    @PostConstruct
    public void sendAccountMessages() {
        Arrays.asList(AccountDataEnum.values())
                .forEach(
                        accountEnum ->  {
                            Account account = AccountDataEnum.getAccount(accountEnum);
                            LOGGER.info(account.toString());
                            sendAccount(account, sourceTopic);
                });
    }

    @Scheduled(fixedRate = 2000)
    private void scheduleFixedRateTask() {
        sendAccountMessages();
    }


        public void sendAccount(Account message, String topic) {
        ListenableFuture<SendResult<String, String>> future =
                    kafkaTemplateAccount.send(topic,message.getAccountId().toString(), message.toString());
        future.addCallback(new ListenableFutureCallback<>() {
            @Override
            public void onSuccess(SendResult<String, String> result) {
                LOGGER.info("Message [{}] delivered with offset {}",
                            message,
                        result.getRecordMetadata().offset());
            }
            @Override
            public void onFailure(Throwable ex) {
                    LOGGER.warn("Unable to deliver message [{}]. {}",
                            message,
                        ex.getMessage());
            }
        });
    }

}

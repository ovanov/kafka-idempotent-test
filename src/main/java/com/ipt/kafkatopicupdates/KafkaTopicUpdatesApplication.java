package com.ipt.kafkatopicupdates;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class KafkaTopicUpdatesApplication {

	public static void main(String[] args) {
		SpringApplication.run(KafkaTopicUpdatesApplication.class, args);
	}

}

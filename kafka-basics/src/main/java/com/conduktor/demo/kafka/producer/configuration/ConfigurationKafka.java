package com.conduktor.demo.kafka.producer.configuration;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class ConfigurationKafka {

    final String TOPIC = "demo_kafka";

    @Bean
    public NewTopic demoKafka(){
        return TopicBuilder
                .name(TOPIC)
                .partitions(3)
                .replicas(3)
                .build();
    }

}

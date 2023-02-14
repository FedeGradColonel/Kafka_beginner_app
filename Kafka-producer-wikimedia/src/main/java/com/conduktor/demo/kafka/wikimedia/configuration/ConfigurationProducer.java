package com.conduktor.demo.kafka.wikimedia.configuration;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class ConfigurationProducer {

    final String TOPIC = "wikimedia.recentchange";

    @Bean
    public NewTopic wikimedia(){
        return TopicBuilder.name(TOPIC)
                .partitions(3)
                .replicas(3)
                .build();
    }

}

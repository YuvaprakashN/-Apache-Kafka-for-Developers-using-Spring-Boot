package com.learnkafka.library_events_producer.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
@Profile("local")
public class AutoTopicCreateConfig {

    @Value("${spring.kafka.topics.library-events}")
    private String topic;

    @Bean
    public NewTopic createLibraryEvent(){
        return TopicBuilder.name(topic).replicas(3).partitions(3).build();
    }
}

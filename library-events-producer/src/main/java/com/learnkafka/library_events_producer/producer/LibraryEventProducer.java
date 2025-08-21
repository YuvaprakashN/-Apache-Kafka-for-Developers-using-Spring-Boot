package com.learnkafka.library_events_producer.producer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.learnkafka.library_events_producer.domain.LibraryEvent;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class LibraryEventProducer {

    private KafkaTemplate<Integer, String> kafkaTemplate;
    private ObjectMapper objectMapper;
    @Value("${spring.kafka.topics.library-events}")
    private String topic;

    public LibraryEventProducer(KafkaTemplate<Integer, String> kafkaTemplate, ObjectMapper objectMapper) {
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
    }

    public void onMessage(LibraryEvent libraryEvent) throws JsonProcessingException {
        log.info("LibraryEvent received: {}", libraryEvent);
        Integer key = libraryEvent.libraryEventId();
        String value = objectMapper.writeValueAsString(libraryEvent);
        // kafkaTemplate.sendDefault( key, value);    spring.kafka.template.default-topic config
        kafkaTemplate.send(topic, key, value);

        log.info("LibraryEvent sent to {}", topic);
    }
}

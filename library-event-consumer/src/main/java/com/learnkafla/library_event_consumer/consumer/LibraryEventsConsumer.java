package com.learnkafla.library_event_consumer.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Slf4j
//@Component
public class LibraryEventsConsumer {

    @KafkaListener(topics = "${spring.kafka.topics.library-events}", groupId = "${spring.kafka.topics.library-events-group-id}")
    public void onMessage(ConsumerRecord<Integer, String> record) {
        log.info("Consume Msg: {}", record);
    }
}

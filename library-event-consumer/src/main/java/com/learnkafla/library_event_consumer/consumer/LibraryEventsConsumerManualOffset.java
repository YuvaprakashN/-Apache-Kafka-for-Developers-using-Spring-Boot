package com.learnkafla.library_event_consumer.consumer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.listener.AcknowledgingMessageListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class LibraryEventsConsumerManualOffset implements AcknowledgingMessageListener<Integer,String> {

    @Override
    @KafkaListener(topics = "${spring.kafka.topics.library-events}", groupId = "${spring.kafka.topics.library-events-group-id}")
    public void onMessage(ConsumerRecord<Integer, String> data, Acknowledgment acknowledgment) {

        acknowledgment.acknowledge();
        log.info("MAnual Ack: {}", data);
    }
}

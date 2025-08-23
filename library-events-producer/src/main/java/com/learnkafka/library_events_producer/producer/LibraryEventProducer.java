package com.learnkafka.library_events_producer.producer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.learnkafka.library_events_producer.domain.LibraryEvent;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.concurrent.CompletableFuture;

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

    public void sendLibraryEvents(LibraryEvent libraryEvent) throws JsonProcessingException {
        log.info("LibraryEvent received: {}", libraryEvent);
        Integer key = libraryEvent.libraryEventId();
        String value = objectMapper.writeValueAsString(libraryEvent);
        // kafkaTemplate.sendDefault( key, value);    spring.kafka.template.default-topic config
        kafkaTemplate.send(topic, key, value);

        log.info("LibraryEvent sent to {}", topic);
    }

    public void sendLibraryEventsWithCompletableFuture(LibraryEvent libraryEvent) throws JsonProcessingException {
        log.info("LibraryEvent received: {}", libraryEvent);
        Integer key = libraryEvent.libraryEventId();
        String value = objectMapper.writeValueAsString(libraryEvent);
        // kafkaTemplate.sendDefault( key, value);    spring.kafka.template.default-topic config
        var completableFuture = kafkaTemplate.send(topic, key, value);

        completableFuture.whenComplete(
                (sendResult, throwable) -> {
                    if (throwable != null) {
                        handleSuccess(key,value, sendResult);
                    } else {
                        handleFailure(key, value, throwable);
                    }
                }
        );

        log.info("LibraryEvent sent to {}", topic);
    }

    public CompletableFuture<SendResult<Integer, String>> sendLibraryEventsWithProducerRecordAndCompletableFuture(LibraryEvent libraryEvent) throws JsonProcessingException {
        log.info("LibraryEvent received: {}", libraryEvent);
        Integer key = libraryEvent.libraryEventId();
        String value = objectMapper.writeValueAsString(libraryEvent);
        // kafkaTemplate.sendDefault( key, value);    spring.kafka.template.default-topic config
        var completableFuture = kafkaTemplate.send(buildProducerRecord(topic,key,value));

        completableFuture.whenComplete(
                (sendResult, throwable) -> {
                    if (throwable != null) {
                        handleSuccess(key,value, sendResult);
                    } else {
                        handleFailure(key, value, throwable);
                    }
                }
        );

        log.info("LibraryEvent sent to {}", topic);
        return completableFuture;
    }

    public ProducerRecord<Integer,String> buildProducerRecord(String topic, Integer key, String value){
        List<Header> recordHeaders= List.of(new RecordHeader("event-source","scanner".getBytes()));

        return new ProducerRecord<>(topic,null,key,value,recordHeaders);
    }

    private void handleFailure(Integer key, String value, Throwable ex) {
        log.error("Error Sending the Message and the exception is {}", ex.getMessage());
//        try {
//            throw ex;
//        } catch (Throwable throwable) {
//            log.error("Error in OnFailure: {}", throwable.getMessage());
//        }
    }

    private void handleSuccess(Integer key, String value, SendResult<Integer, String> result) {
        log.info("Message Sent SuccessFully for the key : {} and the value is {} , partition is {}", key, value, result.getRecordMetadata().partition());
    }
}

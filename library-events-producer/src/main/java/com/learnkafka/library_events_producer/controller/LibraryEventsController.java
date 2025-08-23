package com.learnkafka.library_events_producer.controller;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.learnkafka.library_events_producer.domain.LibraryEvent;
import com.learnkafka.library_events_producer.domain.LibraryEventType;
import com.learnkafka.library_events_producer.producer.LibraryEventProducer;
import jakarta.validation.Valid;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

@Slf4j
@RestController
public class LibraryEventsController {

    @Autowired
    private LibraryEventProducer libraryEventProducer;

    @PostMapping("/v1/libraryevent")
    public ResponseEntity<?> postLibraryEvent(@RequestBody @Valid LibraryEvent libraryEvent) throws JsonProcessingException {
        log.info("postLibraryEvent Invoked");
        if (LibraryEventType.NEW != libraryEvent.libraryEventType()) {
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body("Only NEW event type is supported");
        }
        libraryEventProducer.sendLibraryEventsWithProducerRecordAndCompletableFuture(libraryEvent);
        return ResponseEntity.status(HttpStatus.CREATED).body(libraryEvent);
    }

    @PutMapping("/v1/libraryevent")
    public ResponseEntity<?> putLibraryEvent(@RequestBody @Valid LibraryEvent libraryEvent) throws JsonProcessingException {


        ResponseEntity<String> BAD_REQUEST = validateUpdateLibraryEvent(libraryEvent);
        if (BAD_REQUEST != null) return BAD_REQUEST;

        libraryEventProducer.sendLibraryEventsWithProducerRecordAndCompletableFuture(libraryEvent);
        log.info("after produce call");
        return ResponseEntity.status(HttpStatus.OK).body(libraryEvent);
    }

    private ResponseEntity<String> validateUpdateLibraryEvent(LibraryEvent libraryEvent) {

        if (libraryEvent.libraryEventId() == null) {
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body("Please pass the LibraryEventId");
        }

        if (libraryEvent.book().bookId() == null) {
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body("Please add the book to the library");
        }

        if (!LibraryEventType.UPDATE.equals(libraryEvent.libraryEventType()))  {
            log.info("Inside the if block");
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body("Only UPDATE event type is supported");
        }
        return null;
    }

}

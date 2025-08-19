package com.learnkafka.library_events_producer.domain;


public record LibraryEvent(
        Integer libraryEventId,
        LibraryEventType libraryEventType,
        Book book
) {
}

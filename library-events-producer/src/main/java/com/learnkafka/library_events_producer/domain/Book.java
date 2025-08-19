package com.learnkafka.library_events_producer.domain;

public record Book(
        Integer bookId,
        String bookName,
        String bookAuthor) {
}

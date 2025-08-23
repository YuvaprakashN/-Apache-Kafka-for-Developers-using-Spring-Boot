package com.learnkafla.library_event_consumer.jpa;

import com.learnkafla.library_event_consumer.entity.LibraryEvent;
import org.springframework.data.jpa.repository.JpaRepository;

public interface LibraryEventsRepository extends JpaRepository<LibraryEvent,Integer> {
}

package com.learnkafla.library_event_consumer.jpa;

import com.learnkafla.library_event_consumer.entity.LibraryEvent;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface LibraryEventsRepository extends JpaRepository<LibraryEvent,Integer> {
}

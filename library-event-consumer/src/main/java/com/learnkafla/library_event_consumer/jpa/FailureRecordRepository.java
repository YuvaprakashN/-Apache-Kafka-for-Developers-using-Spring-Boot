package com.learnkafla.library_event_consumer.jpa;


import com.learnkafla.library_event_consumer.entity.FailureRecord;
import org.springframework.data.repository.CrudRepository;

import java.util.List;

public interface FailureRecordRepository extends CrudRepository<FailureRecord,Integer> {

    List<FailureRecord> findAllByStatus(String status);
}
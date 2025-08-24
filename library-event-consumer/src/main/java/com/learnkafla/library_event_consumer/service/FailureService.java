package com.learnkafla.library_event_consumer.service;

import com.learnkafla.library_event_consumer.entity.FailureRecord;
import com.learnkafla.library_event_consumer.jpa.FailureRecordRepository;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.stereotype.Service;

@Service
public class FailureService {

    private FailureRecordRepository failureRecordRepository;

    public FailureService(FailureRecordRepository failureRecordRepository) {
        this.failureRecordRepository = failureRecordRepository;
    }

    public void saveFailedRecord(ConsumerRecord<Integer, String> record, Exception exception, String recordStatus){
        var failureRecord = new FailureRecord(null,record.topic(), record.key(),  record.value(), record.partition(),record.offset(),
                exception.getCause().getMessage(),
                recordStatus);

        failureRecordRepository.save(failureRecord);

    }
}

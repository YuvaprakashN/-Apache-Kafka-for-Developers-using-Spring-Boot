package com.learnkafla.library_event_consumer.config;

import com.learnkafla.library_event_consumer.service.FailureService;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.ExponentialBackoff;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.kafka.ConcurrentKafkaListenerContainerFactoryConfigurer;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.dao.RecoverableDataAccessException;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.ContainerCustomizer;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.*;
import org.springframework.kafka.support.ExponentialBackOffWithMaxRetries;
import org.springframework.util.backoff.FixedBackOff;

import java.util.List;
import java.util.Objects;

@Configuration
@EnableKafka
@Slf4j
public class LibraryEventsConsumerConfig {

    public static final String RETRY = "RETRY";
    public static final String SUCCESS = "SUCCESS";
    public static final String DEAD = "DEAD";
    @Autowired
    KafkaProperties kafkaProperties;

    @Autowired
    KafkaTemplate kafkaTemplate;

    @Value("${spring.kafka.topics.library-events-retry:library-events.RETRY}")
    String retryTopic;

    @Value("${spring.kafka.topics.library-events-dlq:library-events.DLQ}")
    String deadLetterTopic;


    @Autowired
    private FailureService failureService;


    ConsumerRecordRecoverer consumerRecordRecoverer = (record, exception) -> {
        log.error("Exception is : {} Failed Record : {} ", exception, record);
        if (exception.getCause() instanceof RecoverableDataAccessException) {
            log.info("Inside the recoverable logic");
            //Add any Recovery Code here.
            failureService.saveFailedRecord((ConsumerRecord<Integer, String>) record, exception, RETRY);

        } else {
            log.info("Inside the non recoverable logic and skipping the record : {}", record);

        }
    };

    @Bean
    public DeadLetterPublishingRecoverer publishingRecoverer() {
        return new DeadLetterPublishingRecoverer(
                kafkaTemplate,
                (record, ex) -> {
                    log.error("Exception in publishingRecoverer: {}", ex.getMessage(), ex);
                    if (ex.getCause() instanceof RecoverableDataAccessException) {
                        log.info("Publish msg to retry");
                        return new TopicPartition(retryTopic, record.partition());
                    } else {
                        log.info("Publish msg to dlq");
                        return new TopicPartition(deadLetterTopic, record.partition());

                    }

                }
        );
    }

    @Bean
    @ConditionalOnMissingBean(
            name = {"kafkaListenerContainerFactory"}
    )
    ConcurrentKafkaListenerContainerFactory<?, ?> kafkaListenerContainerFactory(ConcurrentKafkaListenerContainerFactoryConfigurer configurer, ObjectProvider<ConsumerFactory<Object, Object>> kafkaConsumerFactory, ObjectProvider<ContainerCustomizer<Object, Object, ConcurrentMessageListenerContainer<Object, Object>>> kafkaContainerCustomizer) {
        ConcurrentKafkaListenerContainerFactory<Object, Object> factory = new ConcurrentKafkaListenerContainerFactory();
        configurer.configure(factory, (ConsumerFactory) kafkaConsumerFactory.getIfAvailable(() -> {
            return new DefaultKafkaConsumerFactory<>(this.kafkaProperties.buildConsumerProperties());
        }));
        Objects.requireNonNull(factory);
        // factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);
        factory.setConcurrency(3);
        factory.setCommonErrorHandler(defaultErrorHandler());
        kafkaContainerCustomizer.ifAvailable(factory::setContainerCustomizer);
        return factory;
    }

    public DefaultErrorHandler defaultErrorHandler() {
        //  FixedBackOff fixedBackoff = new FixedBackOff(1000L, 2);

        var exponentialBackoff = new ExponentialBackOffWithMaxRetries(5);
        exponentialBackoff.setInitialInterval(1000L);
        exponentialBackoff.setMultiplier(2.0);
        exponentialBackoff.setMaxInterval(2000L);

        var defaultErrorHandler = new DefaultErrorHandler(
                consumerRecordRecoverer,
                //publishingRecoverer(),
                exponentialBackoff);

        var exceptionList = List.of(IllegalArgumentException.class);
        exceptionList.forEach(defaultErrorHandler::addNotRetryableExceptions);
        defaultErrorHandler.setRetryListeners(
                ((record, ex, deliveryAttempt) -> {
                    log.info("Failed Record in Retry Listener  exception : {} , deliveryAttempt : {} ", ex.getMessage(), deliveryAttempt);
                })
        );
        return defaultErrorHandler;
    }

}

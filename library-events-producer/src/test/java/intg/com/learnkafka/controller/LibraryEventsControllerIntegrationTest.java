package intg.com.learnkafka.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.learnkafka.library_events_producer.LibraryEventsProducerApplication;
import com.learnkafka.library_events_producer.domain.LibraryEvent;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.*;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.TestPropertySources;
import unit.com.learnkafka.util.com.learnkafka.TestUtil;

import java.util.HashMap;
import java.util.Map;

import static org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG;
import static org.junit.jupiter.api.extension.MediaType.APPLICATION_JSON;

import static org.junit.jupiter.api.Assertions.assertEquals;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,classes = LibraryEventsProducerApplication.class)
@EmbeddedKafka(topics = {"${spring.kafka.topics.library-events}"},partitions = 3)
@TestPropertySource(properties = {"spring.kafka.producer.bootstrap.server=${{spring.embedded.kafka.brokers}",
        "spring.kafka.admin.producer.bootstrap.server=${spring.embedded.kafka.brokers}"})
public class LibraryEventsControllerIntegrationTest {

    @Autowired
    TestRestTemplate testRestTemplate;

    @Autowired
    EmbeddedKafkaBroker embeddedKafkaBroker;

    @Autowired
    ObjectMapper objectMapper;

    private Consumer<Integer, String> consumer;

    @BeforeEach
    void setUp() {
        Map<String, Object> configs = new HashMap<>(KafkaTestUtils.consumerProps("group1", "true", embeddedKafkaBroker));
        configs.put(AUTO_OFFSET_RESET_CONFIG, "latest");
        consumer = new DefaultKafkaConsumerFactory<>(configs, new IntegerDeserializer(), new StringDeserializer()).createConsumer();
        embeddedKafkaBroker.consumeFromAllEmbeddedTopics(consumer);
    }

    @AfterEach
    void tearDown() {
        consumer.close();
    }

    @Test
    void postLibraryEventTest() {
        // given
        LibraryEvent libraryEvent = TestUtil.libraryEventRecord();
        HttpHeaders httpHeaders = new HttpHeaders();
        httpHeaders.add(HttpHeaders.CONTENT_TYPE, APPLICATION_JSON.toString());
        HttpEntity<LibraryEvent> httpEntityRequest = new HttpEntity<>(libraryEvent, httpHeaders);

        ResponseEntity<LibraryEvent> response = testRestTemplate.exchange("/v1/libraryevent", HttpMethod.POST, httpEntityRequest, LibraryEvent.class);

        // Then
        assertEquals(response.getStatusCode(), HttpStatus.CREATED);
        ConsumerRecords<Integer, String> consumerRecord = KafkaTestUtils.getRecords(consumer);

        assertEquals(consumerRecord.count(), 1);
        consumerRecord.forEach(record -> {
            LibraryEvent libraryEvent1 = TestUtil.parseLibraryEventRecord(objectMapper, record.value());
            assertEquals(libraryEvent1, libraryEvent);
        });
    }

}

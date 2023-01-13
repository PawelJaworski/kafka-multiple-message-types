package com.example.kafkamessagemultipletypes;

import com.example.kafkamessagemultipletypes.message.ShipmentDocument;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.context.EmbeddedKafka;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static com.example.kafkamessagemultipletypes.Config.RECEIVER_HEADER;

@SpringBootTest
@Import({KafkaTemplateTestConfig.class})
@EmbeddedKafka(partitions = 1, brokerProperties = { "listeners=PLAINTEXT://localhost:9092", "port=9092" })
class KafkaFilteredListenerIntegrationTest {
    @Autowired
    KafkaTemplate<String, Object> kafkaTemplate;

    @Autowired
    KafkaMessageFilteredReceiverListener kafkaMessageFilteredListener;

    @Value("${kafka.bootstrap-servers}")
    String bootstrapServers;

    @BeforeEach
    void beforeEach() {
        kafkaMessageFilteredListener.documentCountDown = new CountDownLatch(1);
    }

    @Test
    void shouldConsumeShipmentDocumentWithReceiverHeader() throws InterruptedException, ExecutionException {
        //when
        var document = new ShipmentDocument("BIA-WAW", "Bialystok", "Warszawa");

        ProducerRecord<String, Object> producerRecord = new ProducerRecord<>(Config.TOPIC_NAME, document.id(),
                document);
        producerRecord
                .headers()
                .add(RECEIVER_HEADER, "receiver-service".getBytes());

        kafkaTemplate.send(producerRecord).get();
        //then
        var isMessageConsumed = kafkaMessageFilteredListener.documentCountDown
                .await(5, TimeUnit.SECONDS);
        Assertions.assertTrue(isMessageConsumed);
    }

    @Test
    void shouldIgnoreShipmentDocumentWithOtherReceiverHeader() throws InterruptedException, ExecutionException {
        //when
        var document = new ShipmentDocument("BIA-WAW", "Bialystok", "Warszawa");

        ProducerRecord<String, Object> producerRecord = new ProducerRecord<>(Config.TOPIC_NAME, document.id(),
                document);
        producerRecord
                .headers()
                .add(RECEIVER_HEADER, "other-receiver-service".getBytes());

        kafkaTemplate.send(producerRecord).get();
        //then
        var isMessageConsumed = kafkaMessageFilteredListener.documentCountDown
                .await(5, TimeUnit.SECONDS);
        Assertions.assertFalse(isMessageConsumed);
    }
}

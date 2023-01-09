package com.example.kafkamessagemultipletypes;

import com.example.kafkamessagemultipletypes.message.ShipmentDocument;
import com.example.kafkamessagemultipletypes.message.ShipmentLeftEvent;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.context.EmbeddedKafka;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

@SpringBootTest
@Import({KafkaTemplateTestConfig.class})
@EmbeddedKafka(partitions = 1, brokerProperties = { "listeners=PLAINTEXT://localhost:9092", "port=9092" })
class KafkaDefaultListenerIntegrationTest {
    @Autowired
    KafkaTemplate<String, Object> kafkaTemplate;

    @Autowired
    KafkaMessageDefaultListener kafkaMessageDefaultListener;

    @Value("${kafka.bootstrap-servers}")
    String bootstrapServers;

    @Test
    void shouldConsumeShipmentDocument() throws InterruptedException, ExecutionException {
        //when
        var document = new ShipmentDocument("BIA-WAW", "Bialystok", "Warszawa");

        ProducerRecord<String, Object> producerRecord = new ProducerRecord<>(Config.TOPIC_NAME, document.id(),
                document);
        producerRecord
                .headers()
                .add("version", "shipment-document.v1".getBytes());

        kafkaTemplate.send(producerRecord).get();
        //then
        var isMessageConsumed = kafkaMessageDefaultListener.documentCountDown
                .await(5, TimeUnit.SECONDS);
        Assertions.assertTrue(isMessageConsumed);
    }

    @Test
    void shouldConsumeShipmentLeftEvent() throws InterruptedException, ExecutionException {
        //when
        var event = new ShipmentLeftEvent("BIA-WAW");

        kafkaTemplate.send(Config.TOPIC_NAME, event.id(), event);
        //then
        var isMessageConsumed = kafkaMessageDefaultListener.shipmentLeftCountDown
                .await(5, TimeUnit.SECONDS);
        Assertions.assertTrue(isMessageConsumed);
    }
}

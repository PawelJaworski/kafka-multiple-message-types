package com.example.kafkamessagemultipletypes;

import com.example.kafkamessagemultipletypes.message.ShipmentDocumentTestData;
import com.example.kafkamessagemultipletypes.message.ShipmentLeftTestData;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.context.EmbeddedKafka;

import java.util.concurrent.TimeUnit;

@SpringBootTest
@Import({TestConfig.class})
@EmbeddedKafka(partitions = 1, brokerProperties = { "listeners=PLAINTEXT://localhost:9092", "port=9092" })
class KafkaDefaultListenerIntegrationTest {
    @Autowired
    KafkaTemplate<String, Object> kafkaTemplate;

    @Autowired
    KafkaMessageDefaultListener kafkaMessageDefaultListener;

    @Value("${kafka.bootstrap-servers}")
    String bootstrapServers;

    @Test
    void shouldConsumeShipmentDocument() throws InterruptedException {
        //when
        var document = ShipmentDocumentTestData.fromBiaToWaw();
        kafkaTemplate.send(Config.TOPIC_NAME, document.id(), document);
        //then
        var isMessageConsumed = kafkaMessageDefaultListener.documentCountDown
                .await(TestConfig.CONSUMER_TIMEOUT_IN_SEC, TimeUnit.SECONDS);
        Assertions.assertTrue(isMessageConsumed);
    }

    @Test
    void shouldConsumeShipmentLeftEvent() throws InterruptedException {
        //when
        var event = ShipmentLeftTestData.fromBiaToWawLeft();

        kafkaTemplate.send(Config.TOPIC_NAME, event.id(), event);
        //then
        var isMessageConsumed = kafkaMessageDefaultListener.shipmentLeftCountDown
                .await(TestConfig.CONSUMER_TIMEOUT_IN_SEC, TimeUnit.SECONDS);
        Assertions.assertTrue(isMessageConsumed);
    }
}

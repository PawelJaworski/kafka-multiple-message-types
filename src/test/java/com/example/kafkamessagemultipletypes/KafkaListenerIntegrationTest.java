package com.example.kafkamessagemultipletypes;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.ClassRule;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

@SpringBootTest
class KafkaListenerIntegrationTest {

    @Autowired
    KafkaTemplate<String, Object> kafkaTemplate;

    @Autowired
    KafkaMessageListener kafkaMessageListener;

    @ClassRule
    public static KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:5.4.3"));


    @Test
    void shouldConsumeShipmentDocument() throws InterruptedException, ExecutionException {
        //when
        var document = new ShipmentDocument("BIA-WAW", "Bialystok", "Warszawa");
        kafkaTemplate.send(Config.TOPIC_NAME, document.code(), document)
                .get();
        //then
        kafkaMessageListener.documentCountDown
                .await(1, TimeUnit.MILLISECONDS);
    }

    @TestConfiguration
    static class TestConfig {
        public Map<String, Object> producerConfigs() {
            Map<String, Object> props = new HashMap<>();
            props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
            props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
            props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
            return props;
        }

        public ProducerFactory<String, Object> producerFactory() {
            return new DefaultKafkaProducerFactory<>(producerConfigs());
        }

        @Bean
        public KafkaTemplate<String, Object> kafkaTemplate() {
            return new KafkaTemplate<>(producerFactory());
        }
    }

}

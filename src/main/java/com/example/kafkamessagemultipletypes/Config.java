package com.example.kafkamessagemultipletypes;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.support.serializer.JsonDeserializer;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;


@Configuration
class Config {
    static final String TOPIC_NAME = "shipments-topic";
    static final String TRUSTED_PACKAGES = "com.example.kafkamessagemultipletypes.message";
    static final String RECEIVER_HEADER = "receiver";

    @Value("${kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, Object> defaultContainerFactory() {

        ConcurrentKafkaListenerContainerFactory<String, Object> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(new ConsumerFactoryBuilder()
                .groupId("all-events")
                .build());
        return factory;
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, Object> withFilterFactory() {
        ConcurrentKafkaListenerContainerFactory<String, Object> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(new ConsumerFactoryBuilder()
                .groupId("filtered-receiver")
                .build());
        factory.setRecordFilterStrategy(it ->
                Optional.ofNullable(it.headers().lastHeader(RECEIVER_HEADER))
                    .map(Header::value)
                    .map(String::new)
                    .map(receiver -> receiver.compareTo("receiver-service") != 0)
                    .orElse(false));
        return factory;
    }

    private class ConsumerFactoryBuilder {
        final Map<String, Object> props;

        private ConsumerFactoryBuilder() {
            props = new HashMap<>();
            props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            props.put(ConsumerConfig.GROUP_ID_CONFIG, "shipment-group-id");
            props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
            props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
            props.put(JsonDeserializer.TRUSTED_PACKAGES, TRUSTED_PACKAGES);
            props.put(JsonDeserializer.REMOVE_TYPE_INFO_HEADERS, false);
        }

        ConsumerFactoryBuilder groupId(String groupId) {
            props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            return this;
        }

        ConsumerFactory<String, Object> build() {
            return new DefaultKafkaConsumerFactory<>(Map.copyOf(props));
        }
    }
}

package com.example.kafkamessagemultipletypes;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.concurrent.CountDownLatch;

@Component
public class KafkaMessageListener {
    CountDownLatch documentCountDown = new CountDownLatch(1);

    @KafkaListener(topics = Config.TOPIC_NAME)
    void listen(ShipmentDocument document) {
        System.out.println("Document consumed " + document);
//        documentCountDown.countDown();
    }
}

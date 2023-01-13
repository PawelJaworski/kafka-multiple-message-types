package com.example.kafkamessagemultipletypes;

import com.example.kafkamessagemultipletypes.message.ShipmentDocument;
import org.springframework.kafka.annotation.KafkaHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

import java.util.concurrent.CountDownLatch;

@Component
@KafkaListener(topics = Config.TOPIC_NAME, containerFactory = "withFilterFactory")
class KafkaMessageFilteredReceiverListener {
    CountDownLatch documentCountDown = new CountDownLatch(1);

    @KafkaHandler
    public void listen(ShipmentDocument document, @Header("__TypeId__") String type) {
        System.out.println("[Filtered] Type: " + type );
        documentCountDown.countDown();
    }
}

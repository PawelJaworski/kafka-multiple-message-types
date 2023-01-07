package com.example.kafkamessagemultipletypes;

import com.example.kafkamessagemultipletypes.message.ShipmentDocument;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Headers;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.CountDownLatch;

@Component
class KafkaMessageListener {
    CountDownLatch documentCountDown = new CountDownLatch(1);

    @KafkaListener(groupId = "document-listener", properties = {"auto.offset.reset=earliest"},
            topics = Config.TOPIC_NAME, containerFactory = "defaultContainerFactory")
    public void listen(ShipmentDocument document, @Headers Map<String, Object> headers) {
        System.out.println("Headers: " + headers );
        System.out.println("Document consumed: " + document);
        documentCountDown.countDown();
    }
}

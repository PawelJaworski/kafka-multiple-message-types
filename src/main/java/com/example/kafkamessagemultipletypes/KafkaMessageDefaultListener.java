package com.example.kafkamessagemultipletypes;

import com.example.kafkamessagemultipletypes.message.ShipmentDocument;
import com.example.kafkamessagemultipletypes.message.ShipmentLeftEvent;
import org.springframework.kafka.annotation.KafkaHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Headers;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.CountDownLatch;

@Component
@KafkaListener(groupId = "document-default-listener", topics = Config.TOPIC_NAME,
        containerFactory = "defaultContainerFactory")
class KafkaMessageDefaultListener {
    CountDownLatch documentCountDown = new CountDownLatch(1);
    CountDownLatch shipmentLeftCountDown = new CountDownLatch(1);

    @KafkaHandler
    public void listen(ShipmentDocument document, @Header("__TypeId__") String type) {
        System.out.println("Type: " + type );
        documentCountDown.countDown();
    }

    @KafkaHandler
    public void listen(ShipmentLeftEvent event, @Header("__TypeId__") String type) {
        System.out.println("Type: " + type );
        shipmentLeftCountDown.countDown();
    }
}

package com.example.kafkamessagemultipletypes;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.EnableKafka;

@EnableKafka
@SpringBootApplication
public class KafkaMessageMultipleTypesApplication {

	public static void main(String[] args) {
		SpringApplication.run(KafkaMessageMultipleTypesApplication.class, args);
	}

}
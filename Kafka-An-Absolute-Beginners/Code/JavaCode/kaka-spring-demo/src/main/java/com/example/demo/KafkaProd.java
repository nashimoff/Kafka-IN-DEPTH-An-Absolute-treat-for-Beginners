package com.example.demo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class KafkaProd {

    private static final Logger log = LoggerFactory.getLogger(KafkaProd.class);
    private final KafkaTemplate<String, String> kafkaTemplate;

    public KafkaProd(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void sendMessage(String msg) {
        log.info(String.format("Sending message to createTopic Topic:: %s", msg));
        kafkaTemplate.send("createTopic", msg);
    }
}

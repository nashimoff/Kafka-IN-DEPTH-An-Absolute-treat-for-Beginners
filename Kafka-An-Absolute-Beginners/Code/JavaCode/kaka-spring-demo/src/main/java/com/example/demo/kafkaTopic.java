package com.example.demo;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class kafkaTopic {

    public NewTopic createTopic() {
        return TopicBuilder
                .name("createTopic")
                .build();
    }
}
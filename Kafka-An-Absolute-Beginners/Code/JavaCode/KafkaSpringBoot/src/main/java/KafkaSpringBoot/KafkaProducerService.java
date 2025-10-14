package KafkaSpringBoot;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class KafkaProducerService {

    @Autowired
    private KafkaTemplate<String, StudentDetails> kafkaTemplate;

    private static final String TOPIC = "kafakSpringTopic5090";

    public void sendMessage(StudentDetails user) {
        kafkaTemplate.send(TOPIC, user);
    }
}

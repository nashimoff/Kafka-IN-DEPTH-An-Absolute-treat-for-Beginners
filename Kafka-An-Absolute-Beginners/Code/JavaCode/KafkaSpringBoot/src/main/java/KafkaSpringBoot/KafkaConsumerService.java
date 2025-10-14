package KafkaSpringBoot;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class KafkaConsumerService {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @KafkaListener(topics = "kafakSpringTopic5090", groupId = "group_id")
    public void consume(ConsumerRecord<String, StudentDetails> record) {
        try {
            // Convert the StudentDetails object to JSON format using ObjectMapper
            String jsonString = objectMapper.writeValueAsString(record.value());

            // Print JSON data, partition, and offset
            System.out.println("Consumed JSON data: " + jsonString);
            System.out.println("Partition: " + record.partition());
            System.out.println("Offset: " + record.offset());
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
    }
}

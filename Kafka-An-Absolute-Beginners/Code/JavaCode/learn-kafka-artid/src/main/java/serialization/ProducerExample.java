package serialization;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProducerExample {
    public static void main(String[] args) {
        String topicName = "kafakSpringTopic5090";

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:5090");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class.getName());

        KafkaProducer<String, MyData> producer = new KafkaProducer<>(props);

        // Create an instance of MyData with the new structure
        MyData.K3 k3 = new MyData.K3("v31"); // Create the nested K3 object
        MyData data = new MyData("value1", "value2", k3); // Create MyData object

        // Send data with a callback to confirm success and display metadata
        producer.send(new ProducerRecord<>(topicName, "exampleKey", data), (RecordMetadata metadata, Exception exception) -> {
            if (exception == null) {
                System.out.println("Data sent successfully!");
                System.out.println("Topic: " + metadata.topic());
                System.out.println("Partition: " + metadata.partition());
                System.out.println("Offset: " + metadata.offset());
                System.out.println("Timestamp: " + metadata.timestamp());
                System.out.println("Key: exampleKey");
                System.out.println("Value: " + data);
            } else {
                System.err.println("Error sending data: " + exception.getMessage());
                exception.printStackTrace();
            }
        });

        producer.close();
    }
}

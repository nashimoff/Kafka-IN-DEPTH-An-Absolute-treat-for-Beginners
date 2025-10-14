package serialization;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class ConsumerExample {
    @SuppressWarnings("resource")
	public static void main(String[] args) {
        String topicName = "kafakSpringTopic5090";

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:5090");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class.getName());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "my-group");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put("json.deserializer.class", MyData.class.getName()); // Set target class for deserializer

        KafkaConsumer<String, MyData> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(topicName));

        while (true) {
            ConsumerRecords<String, MyData> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, MyData> record : records) {
                System.out.printf("Consumed record with key %s and value %s%n", record.key(), record.value());
            }
        }
    }
}

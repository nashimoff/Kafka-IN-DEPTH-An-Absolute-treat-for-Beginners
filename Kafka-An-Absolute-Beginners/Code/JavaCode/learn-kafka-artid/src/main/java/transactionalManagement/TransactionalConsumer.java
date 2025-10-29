package transactionalManagement;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class TransactionalConsumer {

    public static void main(String[] args) {
        // Kafka Consumer configuration
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:5091,localhost:5092,localhost:5093");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "transactional-consumer-group");

        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        // Set isolation level to read only committed messages
        props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");

        // Create the Kafka consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        // Subscribe to the topic
        consumer.subscribe(Collections.singletonList("transactionmgmnt02"));

        int messageCount = 0; // Initialize message count

        try {
            while (true) {
                // Poll for new records
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

                // Increment the total number of messages read
                messageCount += records.count();

                // Display each consumed message's key, value, and offset
                for (ConsumerRecord<String, String> record : records) {
                    System.out.printf("Consumed record with Key: %s, Value: %s, Partition: %d, Offset: %d%n",
                            record.key(), record.value(), record.partition(), record.offset());
                }

                // Print the total number of messages read so far
                System.out.println("Total messages read so far: " + messageCount);
                
                // Commit the offsets after processing the records
                consumer.commitSync();
            }
        } finally {
            // Close the consumer
            consumer.close();
        }
    }
}

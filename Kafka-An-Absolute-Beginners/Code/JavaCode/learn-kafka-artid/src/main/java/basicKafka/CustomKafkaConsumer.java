package basicKafka;

/*
 * Listing available topics: The code lists all available topics using the AdminClient and displays them to the user.
Listing partitions and offsets: For each selected topic, the code fetches and lists all available partitions and their latest offsets.
User interaction: The user is prompted to:
Select a topic from the list.
Select a partition from the topic's partitions.
Optionally select an offset (or use the latest offset).
Send custom messages continuously until they type exit.
Multiple messages: The user can send as many messages as they want in a loop, and the producer remains active until they decide to exit.
How it works:
The AdminClient is used to fetch metadata such as topics, partitions, and offsets.
The Producer is used to send custom messages based on user input.
The user selects the topic, partition, and offset before sending any messages.
The program keeps running until the user types "exit".
This setup provides an interactive Kafka producer that can handle sending multiple custom messages to specific partitions and offsets.
 */

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class CustomKafkaConsumer {
    private static final String TOPIC = "transactionmgmnt02";
    private static final String GROUP_ID = "windows-vg001";

    @SuppressWarnings("resource")
	public static void main(String[] args) {
        // Configure the Consumer
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:5090,localhost:5091,localhost:5092"); // List your Kafka brokers
        props.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");

        Consumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(TOPIC));

        // Poll for new messages
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record : records) {
            		System.out.printf("Consumed message : %s, Partition : %d, Offset : %d%n",
                    record.value(), record.partition(), record.offset());
            }
        }
    }
}

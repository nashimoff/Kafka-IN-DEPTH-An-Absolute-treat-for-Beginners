package basicKafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.Scanner;
import java.util.concurrent.ExecutionException;

public class KafkaProducer_MultipleMessage {
    private static final String TOPIC = "transactionmgmnt02";

    public static void main(String[] args) {
        // Configure the Producer
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:5090,localhost:5091,localhost:5092"); // List your Kafka brokers
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer<>(props);

        // Create a SimpleDateFormat instance
        SimpleDateFormat sdf = new SimpleDateFormat("HH:mm:ss:SSS");

        // Sending custom messages via console input
        try (Scanner scanner = new Scanner(System.in)) {
            while (true) {
                System.out.println("Enter message (type 'exit' to quit): ");
                String message = scanner.nextLine();

                if ("exit".equalsIgnoreCase(message)) {
                    break;
                }

                // Get the current timestamp
                long timestampMillis = System.currentTimeMillis();
                String formattedTimestamp = sdf.format(new Date(timestampMillis));

                // Send message to Kafka with a timestamp
                ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC, null, timestampMillis, null, message);
                try {
                    RecordMetadata metadata = producer.send(record).get();
                    System.out.printf("Message sent to partition - %d with offset - %d at timestamp - %s%n",
                            metadata.partition(), metadata.offset(), formattedTimestamp);
                } catch (ExecutionException | InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }

        producer.close();
    }
}

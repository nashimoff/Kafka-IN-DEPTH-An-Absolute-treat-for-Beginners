package intarnarchy.kafka;

import java.util.Properties;
import java.util.Scanner;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

public class CustomKafkaProducer {
	private static final String TOPIC = "windows-topic-01";

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

		// Sending custom messages via console input
		try (Scanner scanner = new Scanner(System.in)) {
			while (true) {
				System.out.println("Enter message (type 'exit' to quit): ");
				String message = scanner.nextLine();

				if ("exit".equalsIgnoreCase(message)) {
					break;
				}

				// Send message to Kafka
				ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC, message);
				try {
					RecordMetadata metadata = producer.send(record).get();
					System.out.printf("Message sent to partition - %d with offset - %d%n", metadata.partition(),
							metadata.offset());

				} catch (Exception e) {
					e.printStackTrace();
				}
			}
			producer.close();
		}

	}
}

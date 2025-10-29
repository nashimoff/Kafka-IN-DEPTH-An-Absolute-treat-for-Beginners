package multiThreadKafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class OneProducerMultipleThread {

    public static void main(String[] args) {
        // Kafka producer configuration settings
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:5090,localhost:5091,localhost:5092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        // Create a single Kafka producer instance
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        // Number of messages to send
        int numMessages = 50;
        
        // Number of threads to use for sending messages
        int numThreads = 7;

        // Executor service to manage threads
        ExecutorService executor = Executors.newFixedThreadPool(numThreads);

        for (int i = 0; i < numMessages; i++) {
            String key = "Key Value : " + i;
            String value = "Message is : " + i;
            
            // Submit a task (message send) to the thread pool
            executor.submit(new ProducerTask(producer, "windows-topic-01", key, value));
        }

        // Shut down the executor service gracefully
        executor.shutdown();

        // Ensure producer is closed after use
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            producer.close();
            System.out.println("Producer closed.");
        }));
    }

    static class ProducerTask implements Runnable {
        private final KafkaProducer<String, String> producer;
        private final String topic;
        private final String key;
        private final String value;

        public ProducerTask(KafkaProducer<String, String> producer, String topic, String key, String value) {
            this.producer = producer;
            this.topic = topic;
            this.key = key;
            this.value = value;
        }

        @Override
        public void run() {
            try {
                // Send the message asynchronously
                Future<RecordMetadata> future = producer.send(new ProducerRecord<>(topic, key, value));

                // Wait for the message to be acknowledged
                RecordMetadata metadata = future.get();
                System.out.printf("Sent message (key=%s, value=%s) to partition %d with offset %d%n",
                        key, value, metadata.partition(), metadata.offset());
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}



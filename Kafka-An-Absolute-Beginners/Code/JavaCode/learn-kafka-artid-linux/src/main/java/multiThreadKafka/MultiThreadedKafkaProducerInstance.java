package multiThreadKafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class MultiThreadedKafkaProducerInstance {

    public static void main(String[] args) {
        // Kafka producer configuration settings
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:5090,localhost:5091,localhost:5092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        // Number of threads to use
        int numThreads = 5;

        // Create and start multiple producer threads
        for (int i = 0; i < numThreads; i++) {
            new Thread(new ProducerThread(props, "windows-topic-01", i)).start();
        }
    }

    static class ProducerThread implements Runnable {
        private final KafkaProducer<String, String> producer;
        private final String topic;
        private final int threadNumber;

        public ProducerThread(Properties props, String topic, int threadNumber) {
            this.producer = new KafkaProducer<>(props);
            this.topic = topic;
            this.threadNumber = threadNumber;
        }

        @Override
        public void run() {
            for (int i = 0; i < 10; i++) {
                String key = "Key : " + threadNumber + "-" + i;
                String value = "Message : " + threadNumber + "-" + i;

                ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);

                try {
                    // Synchronous send
                    RecordMetadata metadata = producer.send(record).get();
                    System.out.printf("Thread %d sent message (key=%s, value=%s) to partition %d with offset %d%n",
                            threadNumber, key, value, metadata.partition(), metadata.offset());
                } catch (InterruptedException | ExecutionException e) {
                    e.printStackTrace();
                }
            }
            producer.close();
        }
    }
}


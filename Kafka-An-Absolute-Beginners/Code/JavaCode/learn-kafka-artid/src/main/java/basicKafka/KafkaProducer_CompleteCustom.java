package basicKafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.OffsetSpec;

import java.util.*;
import java.util.concurrent.ExecutionException;

@SuppressWarnings("unused")
public class KafkaProducer_CompleteCustom {

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        // Kafka broker properties
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:5090,localhost:5091,localhost:5092"); // List your Kafka brokers
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        // Display key.serializer and value.serializer
        System.out.println("Key Serializer: " + props.getProperty("key.serializer"));
        System.out.println("Value Serializer: " + props.getProperty("value.serializer"));

        // Create Kafka producer
        Producer<String, String> producer = new KafkaProducer<>(props);

        // AdminClient to fetch metadata
        AdminClient adminClient = AdminClient.create(props);
        try (Scanner scanner = new Scanner(System.in)) {
            // Step 1: List available topics
            ListTopicsResult topicsResult = adminClient.listTopics();
            Set<String> topics = topicsResult.names().get();
            System.out.println("Available topics in the broker:");
            for (String topic : topics) {
                System.out.println(" - " + topic);
            }

            // Step 2: Select a topic
            System.out.print("Select a topic name: ");
            String topicName = scanner.nextLine();

            if (!topics.contains(topicName)) {
                System.out.println("Invalid topic selected.");
                producer.close();
                adminClient.close();
                return;
            }

            // Step 3: List available partitions and offsets for the selected topic
            DescribeTopicsResult describeTopicsResult = adminClient.describeTopics(Collections.singletonList(topicName));
            @SuppressWarnings("deprecation")
            TopicDescription topicDescription = describeTopicsResult.values().get(topicName).get();

            List<TopicPartitionInfo> partitionInfos = topicDescription.partitions();
            System.out.println("Available partitions for topic: " + topicName);
            for (TopicPartitionInfo partitionInfo : partitionInfos) {
                int partitionId = partitionInfo.partition();
                TopicPartition topicPartition = new TopicPartition(topicName, partitionId);

                // Get the latest offset for each partition
                Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> offsets = adminClient
                        .listOffsets(Collections.singletonMap(topicPartition, OffsetSpec.latest())).all().get();

                long latestOffset = offsets.get(topicPartition).offset();
                System.out.println("Partition: " + partitionId + ", Latest Offset: " + latestOffset);
            }

            // Step 4: Allow user to select a partition and offset
            System.out.print("Select partition number: ");
            int selectedPartition = scanner.nextInt();
            scanner.nextLine();  // Consume newline

            System.out.print("Select an offset (or type -1 for the latest offset): ");
            @SuppressWarnings("unused")
            long selectedOffset = scanner.nextLong();
            scanner.nextLine();  // Consume newline

            // Step 5: Allow user to send custom messages
            while (true) {
                System.out.print("Enter your message (type 'exit' to quit): ");
                String message = scanner.nextLine();

                if ("exit".equalsIgnoreCase(message)) {
                    break;
                }

                // Send message to the selected topic, partition, and optionally offset
                ProducerRecord<String, String> record = new ProducerRecord<>(topicName, selectedPartition, null, message);
                try {
                    RecordMetadata metadata = producer.send(record).get();
                    System.out.printf("Message sent to partition %d with offset %d%n",
                            metadata.partition(), metadata.offset());
                } catch (ExecutionException | InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }

        // Close the producer and admin client
        producer.close();
        adminClient.close();
    }
}

package transactionalManagement;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.Scanner;

public class TransactionalProducer {

    public static void main(String[] args) {
        // Kafka Producer configuration
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:6091,localhost:6092,localhost:6093");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Enable idempotence and transactions for exactly-once semantics
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "transactional-producer-id");

        // Create the Kafka Producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        // Initialize the transaction
        producer.initTransactions();

        // Scanner for user input
        Scanner scanner = new Scanner(System.in);

        try {
            // Start the first transaction
            producer.beginTransaction();
            System.out.println("First transaction started...");

            while (true) {
                // Get user input for key and value
                System.out.print("Enter message key (or type 'commit' to commit, 'abort' to abort and exit): ");
                String key = scanner.nextLine();
                
                if (key.equalsIgnoreCase("commit")) {
                    // Commit the transaction if the user types 'commit'
                    producer.commitTransaction();
                    System.out.println("Transaction committed.");
                    break;
                } else if (key.equalsIgnoreCase("abort")) {
                    // Abort the transaction if the user types 'abort'
                    producer.abortTransaction();
                    System.out.println("Transaction aborted.");
                    break;
                }

                System.out.print("Enter message value: ");
                String value = scanner.nextLine();

                // Send the custom message entered by the user
                producer.send(new ProducerRecord<>("transactionmgmnt03", key, value));
                System.out.println("Message sent with Key: " + key + ", Value: " + value);
            }

        } catch (ProducerFencedException e) {
            System.err.println("Producer fenced: " + e.getMessage());
            producer.close();  // In case of fencing, close the producer
        } catch (KafkaException e) {
            // Abort the transaction due to any failure
            System.err.println("Transaction failed, aborting: " + e.getMessage());
            producer.abortTransaction();
        } finally {
            // Close the producer
            producer.close();
            scanner.close();
        }
    }
}

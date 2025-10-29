package essentails;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.errors.TopicExistsException;

import java.util.*;
import java.util.concurrent.ExecutionException;

public class KafkaAdministrator {

	private static AdminClient adminClient;

	public static void main(String[] args) {
		// Set up properties for AdminClient with multiple brokers
		Properties properties = new Properties();

		// Replace with your broker addresses
		properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:6091,localhost:6092,localhost:6093");

		// Initialize AdminClient
		adminClient = AdminClient.create(properties);

		@SuppressWarnings("resource")
		Scanner scanner = new Scanner(System.in);
		while (true) {
			System.out.println("\n");
			System.out.println("1. List brokers");
			System.out.println("2. Select broker to list topics");
			System.out.println("3. Create a topic");
			System.out.println("4. Exit");
			System.out.print("Choose an option: ");
			int choice = scanner.nextInt();
			switch (choice) {
			case 1:
				listBrokers();
				break;
			case 2:
				listBrokers();
				System.out.print("\nEnter the broker ID to list topics: ");
				int brokerId = scanner.nextInt();
				listTopicsForBroker(brokerId);
				break;
			case 3:
				createTopic();
				break;
			case 4:
				shutdown();
				return;
			default:
				System.out.println("\nInvalid option, try again.");
			}
		}
	}

	// Method to list available brokers
	private static void listBrokers() {
		try {
			DescribeClusterResult clusterResult = adminClient.describeCluster();
			Collection<Node> nodes = clusterResult.nodes().get();
			System.out.println("\nAvailable Brokers:");
			for (Node node : nodes) {
				System.out.println("Broker ID: " + node.id() + " | Host: " + node.host() + " | Port: " + node.port());
			}
		} catch (InterruptedException | ExecutionException e) {
			e.printStackTrace();
		}
	}

	// Method to list topics for a specific broker
	private static void listTopicsForBroker(int brokerId) {
		try {
			DescribeClusterResult clusterResult = adminClient.describeCluster();
			Collection<Node> nodes = clusterResult.nodes().get();

			// Check if the broker ID exists
			Node broker = nodes.stream().filter(node -> node.id() == brokerId).findFirst().orElse(null);
			if (broker == null) {
				System.out.println("\nBroker with ID " + brokerId + " not found.");
				return;
			}

			// List topics across the cluster (topics are distributed across brokers)
			ListTopicsResult listTopicsResult = adminClient.listTopics();
			Set<String> topics = listTopicsResult.names().get();

			System.out.println("\nAvailable Topics for Broker ID " + brokerId + ":");
			for (String topic : topics) {
				describeOrDeleteTopic(topic);
			}
		} catch (InterruptedException | ExecutionException e) {
			e.printStackTrace();
		}
	}

	// Method to create a topic
	private static void createTopic() {
		@SuppressWarnings("resource")
		Scanner scanner = new Scanner(System.in);
		
		// Input topic name
		System.out.print("Enter the topic name: ");
		String topicName = scanner.next();

		// Input number of partitions
		System.out.print("Enter the number of partitions: ");
		int numPartitions = scanner.nextInt();

		// Input replication factor
		System.out.print("Enter the replication factor: ");
		short replicationFactor = scanner.nextShort();

		// Ask if the topic should be created on one broker or all available brokers
		System.out.println("1. Create on a specific broker");
		System.out.println("2. Create on all available brokers");
		System.out.print("Choose where to create the topic: ");
		int brokerChoice = scanner.nextInt();

		// Create topic with provided parameters
		try {
			NewTopic newTopic = new NewTopic(topicName, numPartitions, replicationFactor);

			if (brokerChoice == 1) {
				listBrokers();
				System.out.print("\nEnter the broker ID to create the topic on: ");
				int brokerId = scanner.nextInt();

				// Broker selection logic (for learning purposes, but Kafka itself handles broker assignments)
				DescribeClusterResult clusterResult = adminClient.describeCluster();
				Collection<Node> nodes = clusterResult.nodes().get();
				Node broker = nodes.stream().filter(node -> node.id() == brokerId).findFirst().orElse(null);

				if (broker == null) {
					System.out.println("Broker with ID " + brokerId + " not found.");
					return;
				} else {
					System.out.println("\nCreating topic on broker ID " + brokerId + "...");
				}
			} else if (brokerChoice == 2) {
				System.out.println("\nCreating topic on all available brokers...");
			}

			// Actually create the topic
			adminClient.createTopics(Collections.singleton(newTopic)).all().get();
			System.out.println("Topic '" + topicName + "' created successfully.");
		} catch (TopicExistsException e) {
			System.out.println("Topic already exists: " + topicName);
		} catch (InterruptedException | ExecutionException e) {
			e.printStackTrace();
		}
	}

	// Method to describe or delete a topic
	private static void describeOrDeleteTopic(String topic) {
		@SuppressWarnings("resource")
		Scanner scanner = new Scanner(System.in);
		System.out.println("Topic: " + topic);
		System.out.println("\n");
		System.out.println("1. Describe topic");
		System.out.println("2. Delete topic");
		System.out.print("Choose an option for topic " + topic + ": ");
		int option = scanner.nextInt();
		switch (option) {
		case 1:
			describeTopic(topic);
			break;
		case 2:
			deleteTopic(topic);
			break;
		default:
			System.out.println("Invalid option.");
		}
	}

	// Describe a topic
	private static void describeTopic(String topic) {
		try {
			DescribeTopicsResult describeTopicsResult = adminClient.describeTopics(Collections.singletonList(topic));
			@SuppressWarnings("deprecation")
			Map<String, TopicDescription> descriptionMap = describeTopicsResult.all().get();
			TopicDescription description = descriptionMap.get(topic);
			System.out.println("\n");
			System.out.println("Topic Description: " + description);
		} catch (InterruptedException | ExecutionException e) {
			e.printStackTrace();
		}
	}

	// Delete a topic
	private static void deleteTopic(String topic) {
		try {
			DeleteTopicsResult deleteTopicsResult = adminClient.deleteTopics(Collections.singletonList(topic));
			deleteTopicsResult.all().get();
			System.out.println("Topic " + topic + " deleted.");
		} catch (InterruptedException | ExecutionException e) {
			e.printStackTrace();
		}
	}

	// Shutdown AdminClient gracefully
	private static void shutdown() {
		adminClient.close();
		System.out.println("Shutting down.");
	}
}

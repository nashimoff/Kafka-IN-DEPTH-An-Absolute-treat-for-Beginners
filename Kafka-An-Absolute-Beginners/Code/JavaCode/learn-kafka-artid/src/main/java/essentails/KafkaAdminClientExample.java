package essentails;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.Node;

import java.util.*;
import java.util.concurrent.ExecutionException;

public class KafkaAdminClientExample {

	private static AdminClient adminClient;

	public static void main(String[] args) {
		// Set up properties for AdminClient with multiple brokers
		Properties properties = new Properties();

		// Replace with your broker addresses
		properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:6091");

		// Initialize AdminClient
		adminClient = AdminClient.create(properties);

		@SuppressWarnings("resource")
		Scanner scanner = new Scanner(System.in);
		while (true) {
			
			System.out.println("1. List brokers");
			System.out.println("2. Select broker to list topics");
			System.out.println("3. Exit");
			System.out.print("Choose an option: ");
			int choice = scanner.nextInt();
			switch (choice) {
			case 1:
				listBrokers();
				break;
			case 2:
				listBrokers();
				System.out.print("\n Enter the broker ID to list topics: ");
				int brokerId = scanner.nextInt();
				listTopicsForBroker(brokerId);
				break;
			case 3:
				shutdown();
				return;
			default:
				System.out.println("Invalid option, try again.");
			}
		}
	}

	// Method to list available brokers
	private static void listBrokers() {
		try {
			DescribeClusterResult clusterResult = adminClient.describeCluster();
			Collection<Node> nodes = clusterResult.nodes().get();
			System.out.println("\n Available Brokers:");
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
				System.out.println("\n Broker with ID " + brokerId + " not found.");
				return;
			}

			// List topics across the cluster (topics are distributed across brokers)
			ListTopicsResult listTopicsResult = adminClient.listTopics();
			Set<String> topics = listTopicsResult.names().get();

			System.out.println("Available Topics for Broker ID " + brokerId + ":");
			for (String topic : topics) {
				describeOrDeleteTopic(topic);
			}
		} catch (InterruptedException | ExecutionException e) {
			e.printStackTrace();
		}
	}

	// Method to describe or delete a topic
	private static void describeOrDeleteTopic(String topic) {
		@SuppressWarnings("resource")
		Scanner scanner = new Scanner(System.in);
		System.out.println("Topic: " + topic);
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

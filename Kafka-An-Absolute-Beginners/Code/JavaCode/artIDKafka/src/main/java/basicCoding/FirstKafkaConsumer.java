package basicCoding;

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;


public class FirstKafkaConsumer {
	
	public static void main(String[] args) {
		
		String brokerHosts = "localhost:6091,localhost:6092,localhost:6093";		
		String topic = "javacode-topic";
		String groupId = "firstJavaKafkaGroup";
		
		Properties einstien = new Properties();
		
		einstien.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,brokerHosts);
		einstien.put(ConsumerConfig.GROUP_ID_CONFIG,groupId);
		einstien.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer");
		einstien.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer");
		einstien.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
		
		//Create Kafaka Consumer Instance
		KafkaConsumer<String, String> consumer = new KafkaConsumer<>(einstien);
		
		//Subscribe to that topic
		consumer.subscribe(Collections.singletonList(topic));
		
		//Code to read the 9 records
		try {
			
			
			boolean checkMessages = true;
			
			while(checkMessages) {
				
				ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(2000));
				
				if(records.isEmpty()) {
					checkMessages = false;
					System.out.println("Records are empty so plz, send something from producer application");
				}
				else {
					for(ConsumerRecord<String, String> saveConsumedValue : records) {
						System.out.printf("\nReceived Record (key = %s value =%s, partition = %d, offset = %d) %n", 
								saveConsumedValue.key(),
								saveConsumedValue.value(),
								saveConsumedValue.partition(),
								saveConsumedValue.offset());
					}
				}
			}			
		} catch(Exception e) {
			e.printStackTrace();
		} finally {
			consumer.close();
		}
		
	}
}

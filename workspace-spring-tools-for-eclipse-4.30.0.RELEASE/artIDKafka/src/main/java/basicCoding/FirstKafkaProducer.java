package basicCoding;

import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

public class FirstKafkaProducer {
	
	public static void main(String[] args) {
		
		String brokerHosts = "172.30.175.132:6091,172.30.175.132:6092,172.30.175.132:6093";
		String topic = "javacode-topic";
		
		Properties einstein = new Properties();
		
		einstein.put("bootstrap.servers", brokerHosts);
		einstein.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		einstein.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		
		//Create Kafka Producer Instance
		KafkaProducer<String, String> producer = new KafkaProducer<>(einstein);
		
		for(int i=0;i<5;i++) {
			String key = "Key ::::: - " + i;
			String value = "message ::::: - " +i;
			
			//Create Producer record
			ProducerRecord<String, String> sendRecord = new ProducerRecord<>(topic,key,value);
		
		
		try {
			//Send Records to Kafka Topic
			
			RecordMetadata metadata = producer.send(sendRecord).get();
			System.out.printf("\nSent Record (key = %s value = %s) meta(partition = %d, offset = %d) %n",
					sendRecord.key(),
					sendRecord.value(),
					metadata.partition(),
					metadata.offset());
			
		} catch (Exception e) {
			e.printStackTrace();
		}
		}
		producer.close();
	}

}

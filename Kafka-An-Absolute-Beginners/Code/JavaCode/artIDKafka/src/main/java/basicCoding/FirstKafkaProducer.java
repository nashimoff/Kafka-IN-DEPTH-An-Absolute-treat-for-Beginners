package basicCoding;

import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

public class FirstKafkaProducer {
	
		public static void main(String[] args) {
			
			String brokerHosts = "localhost:6091,localhost:6092,localhost:6093";
			String topic = "javacode-topic";
			
			Properties einstien = new Properties();
			
			einstien.put("bootstrap.servers", brokerHosts);
			einstien.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
			einstien.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer");
			
			//Create Kafaka Producer Instance
			KafkaProducer<String, String> producer = new KafkaProducer<>(einstien);
			
			for(int i=0;i < 5;i++) {
				String key = "Key :::: - " + i;
				String value = "message :::: - " +i;
				
				//Create Producer record
				ProducerRecord<String, String> sendRecord = new  ProducerRecord<>(topic,key,value);
						
			try {
				
				//Send Records to Kakfa Topic				
				RecordMetadata metadata = producer.send(sendRecord).get();
				System.out.printf("\nSent Record (key = %s value =%s) meta(partition = %d, offset = %d) %n", 
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
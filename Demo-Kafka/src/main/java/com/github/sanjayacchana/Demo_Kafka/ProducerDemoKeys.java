package com.github.sanjayacchana.Demo_Kafka;

import java.io.ObjectInputStream.GetField;
import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProducerDemoKeys {
	public static void main(String [] args) {
		final Logger log = LoggerFactory.getLogger(ProducerDemoKeys.class);
		String bootstrapServer = "127.0.0.1:9092";
		//define properties for producer
		Properties props = new Properties();
		
		props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
		props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		
		// create a producer for kafka with key and value 
		
		KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);
		String topic = "first_topic";
		//create a producer record for kafka 
		for(int i =0;i<5;i++) {
			String key = "Id_"+Integer.toString(i);
			String value = "Hello Sanjay" +Integer.toString(i);
			ProducerRecord<String, String > records = new ProducerRecord<String, String>(topic,key,value);
			
			log.info("key: "+key +"\n");
			producer.send(records,new Callback() {
				
				public void onCompletion(RecordMetadata metadata, Exception exception) {
					// this invokes when a record sent successfully or an exeption is occurred.
					if(exception == null) {
						log.info("Record MetaData is Received \n" +
								"Topic:" +metadata.topic() +"\n"+
								"Partitions:" +metadata.partition()+"\n"+
								"TimeStamp:" +metadata.timestamp());
						
					} else {
						log.error("An Exception is Thrown: "+exception);
					}
					
				}
			});
		}
		
		//flush the data  and close the producer
		
		producer.flush();
		producer.close();
		
		
	}
}

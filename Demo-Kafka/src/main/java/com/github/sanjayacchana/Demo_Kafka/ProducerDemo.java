package com.github.sanjayacchana.Demo_Kafka;

import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProducerDemo {
	static Logger logger = LoggerFactory.getLogger(ProducerDemo.class);
	public static void main(String[] args) {
		String bootStrap_Servers = "127.0.0.1:9092";
		
		
		//create the producer properties
		Properties props = new Properties();
		
		// commented are old way of defining a properties (13-17) 
		/*
		 * props.setProperty("bootstrap.servers", "127.0.0.1:9092");
		 * props.setProperty("key.serializer", StringSerializer.class.getName());
		 * props.setProperty("value.serializer", StringSerializer.class.getName());
		 */
		props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrap_Servers);
		props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		
		
		// create the producer
		
		KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);
			for(int i=0;i<5;i++) {
			// create A Records for producer 
			
			ProducerRecord<String, String> record = new ProducerRecord<String, String>("first_topic", "Hello Sanjay"+Integer.toString(i));
			
			//send data
			
			producer.send(record,new Callback() {
				
				public void onCompletion(RecordMetadata metadata, Exception exception) {
					// executes every time when record is suceesfully sent or exception thrown
					
					if (exception == null) {
						logger.info("Received new meta Data. \n"+
								"Topic:" +metadata.topic() +"\n"+
								"Partitions:" +metadata.partition()+"\n"+
								"TimeStamp:" +metadata.timestamp());
					} else {
						logger.error("Thrown EXception:"+exception);
					}
					
				}
			});
		}
		
		//flush and close 
		producer.flush();
		producer.close();
		
		
	}

}

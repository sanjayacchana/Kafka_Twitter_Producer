package com.github.sanjayacchana.Demo_Kafka;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.LoggerFactory;

public class ConsumerDemo {
	public static void main(String [] args) {
		org.slf4j.Logger log =  LoggerFactory.getLogger(ConsumerDemo.class.getClass());
		String bootstrapServer = "127.0.0.1:9092";
		String topic = "first_topic";
		//define properties config for consumer
		
		Properties props = new Properties();
		props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
		props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
		props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
		props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "my-app-01");
		
		
		//create kafka consumer 
		
		KafkaConsumer<String,String> consumer  = new KafkaConsumer<String, String>(props);
		
		
		//subscribe consumers to our topic(s)
		
		consumer.subscribe(Arrays.asList(topic));
		
		
		//poll for new data 
		while(true) {
			//consumer.poll(100); which is deprecated
			
			ConsumerRecords<String, String> records = 
										consumer.poll(Duration.ofMillis(100)); // from kafka 2.0.0
			
			for(ConsumerRecord<String, String> record : records) {
				log.info("Key: "+record.key() + " Value: " + record.value());
				log.info("Partitions: "+record.partition() + " Offset: "+record.offset());
			}
			
			
		}
		
	}

}

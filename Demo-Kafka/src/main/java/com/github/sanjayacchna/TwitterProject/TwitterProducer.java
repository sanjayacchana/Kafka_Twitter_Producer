package com.github.sanjayacchna.TwitterProject;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;

public class TwitterProducer {
	org.slf4j.Logger log = LoggerFactory.getLogger(TwitterProducer.class.getName());
	
	String  consumerKey = "";
	String consumerSecret = "";
	String token = "";
	String secret = "";
	
	public TwitterProducer() {
		
	}
	public static void main(String[] args) {
		TwitterProducer obj =new TwitterProducer();
		obj.run();
	}
	public void run() {
		//create a twitter producer 
		
		/** Set up your blocking queues: Be sure to size these properly based on expected TPS of your stream */
		BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(100000);
		
		Client client = createKafkaProducer(msgQueue);
		// Attempts to establish a connection.
		client.connect();
		
		//create a kafka producer 
		
		KafkaProducer<String,String> producer =createKakfaProduer();
		
		//add shutdown hook
		Runtime.getRuntime().addShutdownHook(new Thread(() -> {
			log.info("Closinng  kakfa Client.... ");
			client.stop();
			log.info("Closing a kafka Producer...");
			producer.flush();
			producer.close();
			log.info("An Twitter Producer is Closed" );
		}));
		
		//loop to send tweets from kafka
		
		while (!client.isDone()) {
			  String msg =  null;
			  try {
				  msg = msgQueue.poll(5,TimeUnit.SECONDS);
				  
			  }
			  catch(Exception e ) {
				  e.printStackTrace();
				  client.stop();
			  }
			  if (msg != null) {
				log.info(msg); 
				producer.send(new ProducerRecord<String, String>("Twitter_tweets", null, msg),new Callback() {
					
					public void onCompletion(RecordMetadata metadata, Exception exception) {
						// TODO Auto-generated method stub
						if (exception != null) {
							exception.printStackTrace();
							log.error("An Exception is thrown"+exception);
						}
					}
				});
			  }
			}
		log.info("End of Twwets Application");
	}
	public Client createKafkaProducer(BlockingQueue<String> msgQueue) {
		
		
		/** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
		Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
		StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
		// Optional: set up some followings and track terms
		//List<Long> followings = Lists.newArrayList(1234L, 566788L);
		List<String> terms = Lists.newArrayList("kfg2");
		//hosebirdEndpoint.followings(followings);
		hosebirdEndpoint.trackTerms(terms);

		// These secrets should be read from a config file
		Authentication hosebirdAuth = new OAuth1(consumerKey, consumerSecret, token, secret);
		
		ClientBuilder builder = new ClientBuilder()
				  .name("Hosebird-Client-01")                              // optional: mainly for the logs
				  .hosts(hosebirdHosts)
				  .authentication(hosebirdAuth)
				  .endpoint(hosebirdEndpoint)
				  .processor(new StringDelimitedProcessor(msgQueue));
				 // .eventMessageQueue(eventQueue);                          // optional: use this if you want to process client events

				Client hosebirdClient = builder.build();
				return hosebirdClient;
				
	}
	public KafkaProducer<String, String>  createKakfaProduer() {
		String bootstrapServer = "127.0.0.1:9092";
		//define properties for producer
		Properties props = new Properties();
		
		props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
		props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		
		// create a producer for kafka with key and value 
		
		KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);
		return producer;
	}
}

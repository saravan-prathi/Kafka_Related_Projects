package com.github.saravanprathi.kafkaHandson.twitterapi;

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
import org.slf4j.Logger;
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
	Logger logger=LoggerFactory.getLogger(TwitterProducer.class.getName());
	TwitterProducer(){
		
	}
public static void main(String[] args) {
	new TwitterProducer().run();
}
public void run() {
	/** Set up your blocking queues: Be sure to size these properly based on expected TPS of your stream */
	BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(1000);
	//Create Twitter Client
	Client client=createTwitterClient(msgQueue);
	client.connect();
	String msg;
	//Create a kafka Producer
	KafkaProducer<String,String> kafkaProducer=createProducer();
	
	//Adding a shutdown hook
	Runtime.getRuntime().addShutdownHook(new Thread(() -> {
		logger.info("Stopping application");
		logger.info("Closing Client");
		client.stop();
		logger.info("Closing Producer");
		kafkaProducer.close();
	}));
	
	
	//Loop to Send Tweets to kafka
	logger.info("Setup");
	while (!client.isDone()) {
		msg=null;
		  try {
			msg = msgQueue.poll(5, TimeUnit.SECONDS);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			client.stop();
		}
		  if(msg!=null) {
			  
		  	  logger.info(msg);
		  	  kafkaProducer.send(new ProducerRecord<String,String> ("twitter_tweets",null,msg), new Callback() {

				@Override
				public void onCompletion(RecordMetadata metadata, Exception exception) {
					if (exception!=null) {
					logger.error("something bad happened",exception);
					}
		  		  
		  	  	}
			});
		  	  
		  }

		}
	logger.info("End of Application");
	}	

private KafkaProducer<String, String> createProducer() {
	
	String bootstrapServers="127.0.0.1:9092";
	Properties properties=new Properties();
	properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
	properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
	properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
	// TODO Auto-generated method stub
	KafkaProducer<String,String> producer=new KafkaProducer<String,String>(properties);
	
	return producer;
}
public Client createTwitterClient(BlockingQueue<String> msgQueue) {
	
	
	/** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
	Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
	StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
	// Optional: set up some followings and track terms
	List<String> terms = Lists.newArrayList("bitcoin");
	String consumerKey="9Z215G69XWwZRSCNwoiU6xqbx";//Replace appropriately
	String consumerSecret="";//Replace appropriately
	String token="54478227-geYKGf8oSrjK9fBZ47ThH8cOOevjEwFjBMLS9H7af";//Replace appropriately
	String secret="";//Replace appropriately
	hosebirdEndpoint.trackTerms(terms);

	// These secrets should be read from a config file
	Authentication hosebirdAuth = new OAuth1(consumerKey, consumerSecret, token, secret);

	ClientBuilder builder = new ClientBuilder()
			  .name("TwitterClient")                              // optional: mainly for the logs
			  .hosts(hosebirdHosts)
			  .authentication(hosebirdAuth)
			  .endpoint(hosebirdEndpoint)
			  .processor(new StringDelimitedProcessor(msgQueue));
	
			Client hosebirdClient = builder.build();
			return hosebirdClient;
	
}



}

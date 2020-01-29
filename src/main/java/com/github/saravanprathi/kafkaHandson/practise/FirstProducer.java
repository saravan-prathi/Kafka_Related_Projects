package com.github.saravanprathi.kafkaHandson.practise;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class FirstProducer {

	public static void main(String[] args) {
		String bootstrapServers="127.0.0.1:9092";
		Properties properties=new Properties();
		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		// TODO Auto-generated method stub
		KafkaProducer<String,String> producer=new KafkaProducer<String,String>(properties);
		ProducerRecord<String, String> record=new ProducerRecord<String,String>("first_topic","from Java4");
		producer.send(record);
		producer.flush();
		producer.close();
	}
}

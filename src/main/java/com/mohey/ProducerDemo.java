package com.mohey;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProducerDemo {
    public static void main(String[] args) {

        String bootStrapServers = "127.0.0.1:9092";
        //Create Producer Properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //Create The producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        //Create Producer Record
        ProducerRecord<String, String> record = new ProducerRecord<>("first_topic", "Hello World!");

        //Send data - asynchronous
        producer.send(record);

        //Close Connection and Flush
        producer.close();

    }
}

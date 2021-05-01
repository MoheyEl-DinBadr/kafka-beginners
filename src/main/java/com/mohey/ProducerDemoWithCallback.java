package com.mohey;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallback {
    public static void main(String[] args) {

        Logger logger = LoggerFactory.getLogger(ProducerDemoWithCallback.class);
        String bootStrapServers = "127.0.0.1:9092";
        //Create Producer Properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //Create The producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        for(int i=0; i<20; i++){
            //Create Producer Record
            ProducerRecord<String, String> record = new ProducerRecord<>("first_topic", "Hello World" + i);

            //Send data - asynchronous
            producer.send(record, (metadata, exception)->{
                // executes everytime data sent successfully or an exception is thrown
                if(exception == null){
                    //Record is send successfully
                    logger.info("Received new metadata\n "+
                            "Topic: " + metadata.topic() + "\n" +
                            "Partition: " + metadata.partition() + "\n" +
                            "Offsets: " + metadata.offset() + "\n" +
                            "Timestamp: " + metadata.timestamp());
                }else{
                    logger.error("Error while producing", exception);
                }
            });
        }


        //Close Connection and Flush
        producer.close();

    }
}

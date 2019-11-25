package com.simpleStep.kafka.basicOperations;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallback {
    public static void main(String[] args) {

        final Logger logger = LoggerFactory.getLogger(ProducerDemoWithCallback.class);

        //create producer properties
        Properties properties = new Properties();
        String bootstrapServers = "127.0.0.1:9092";
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());


        //create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        for(int i=0; i<10;i++) {
            //create a producer record
            ProducerRecord<String, String> record = new ProducerRecord<String, String>("first_topic", "hello_world " + i);

            //send data - asyncronous
            producer.send(record, new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    //executes every time a record is sent successfully or an exception is thrown.
                    if (e == null) {
                        logger.info("Received new  metadata. \n" + "Topic: " + recordMetadata.topic() + "\n" + "Partition: " + recordMetadata.partition() + "\n" + "offset: " + recordMetadata.offset() + "\n" + "TimeStamp: " + recordMetadata.timestamp());
                    } else {
                        logger.error("Error while Producing", e);
                    }
                }
            });
        }
            //flush data
            producer.flush();

            //flush and close the producer
            producer.close();

    }
}

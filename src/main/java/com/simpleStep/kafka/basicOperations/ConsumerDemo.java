package com.simpleStep.kafka.basicOperations;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemo {
    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(ConsumerDemo.class.getName());

        Properties properties = new Properties();
        String bootstrapServers = "127.0.0.1:9092";
        String groupId = "my-second-application";
        String topic = "first_topic";
        //create consumer configs
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");

        //create consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

        //subscribe consumer to our topic(s)
        consumer.subscribe(Arrays.asList(topic));
        //consumer.subscribe(Collections.singleton(topic));   --> listen to only and only one topic
        //Arrays.asList("first_topic", "second_topic")    --> this way we could also subscribe to multiple topics

        //poll for new data
        while(true)
        {
            ConsumerRecords<String, String> records =consumer.poll(Duration.ofMillis(100)); //new in Kafka 2.0.0
            for (ConsumerRecord<String, String> record:records
                 ) {
                    logger.info("Key :"+record.key() + "\tValue: "+ record.value());
                    logger.info("Partition: ",record.partition());
                    logger.info("Offsets: ",record.offset());
            }

        }
    }
}

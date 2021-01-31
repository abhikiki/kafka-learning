package com.github.kafka.learning.start;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class ConsumerDemoGroups {
    static Logger logger = LoggerFactory.getLogger(ConsumerDemoGroups.class);
    private static final String BOOTSTRAP_SERVER = "localhost:9092";

    public static void main(String[] args) {
        String groupId = "my_fifth_application";
        String topic= "first_topic";
        // create consumer properties

        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");  //earliest,latest or none

        //create consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

        //subscribe consumer to the topic(s)
        consumer.subscribe(Collections.singleton(topic));
        //poll for data
        while(true){
            ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(100));
            for(ConsumerRecord record : consumerRecords){
                logger.info("Key: " + record.key() +" Value: " + record.value());
                logger.info("partition: " + record.partition() + ", offset: " + record.offset());
            }
        }
    }
}

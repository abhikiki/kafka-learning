package com.github.kafka.learning.start;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemoAssignSeek {
    static Logger logger = LoggerFactory.getLogger(ConsumerDemoAssignSeek.class);
    private static final String BOOTSTRAP_SERVER = "localhost:9092";

    public static void main(String[] args) {
        String topic= "first_topic";
        // create consumer properties

        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");  //earliest,latest or none

        //create consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);
        //assign and seek is used to replay data or fetch specific message
        TopicPartition topicPartition = new TopicPartition(topic, 0);
        consumer.assign(Arrays.asList(topicPartition));
        long offsetToReadFrom = 15L;

        consumer.seek(topicPartition, offsetToReadFrom);

        int numberOfMessagesToRead = 5;
        int numberOfMessagesReadSoFar = 0;
        boolean kepOnReading = true;

        //poll for data
        while(kepOnReading){
            ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(100));
            for(ConsumerRecord record : consumerRecords){
                numberOfMessagesReadSoFar++;
                logger.info("Key: " + record.key() +" Value: " + record.value());
                logger.info("partition: " + record.partition() + ", offset: " + record.offset());
                if(numberOfMessagesReadSoFar >= numberOfMessagesToRead){
                    kepOnReading = false;
                    break;
                }
            }
        }
        logger.info("Application is exiting");
    }
}

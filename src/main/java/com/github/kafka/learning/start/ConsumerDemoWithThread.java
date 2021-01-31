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
import java.util.concurrent.CountDownLatch;

public class ConsumerDemoWithThread {
    static Logger logger = LoggerFactory.getLogger(ConsumerDemoWithThread.class);
    private static final String BOOTSTRAP_SERVER = "localhost:9092";
    private static final String topic = "first_topic";

    public static void main(String[] args) {
        String groupId = "my_fourth_application";
        // create consumer properties

        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");  //earliest,latest or none


    }

    public class ConsumerThread implements Runnable{

        private CountDownLatch countDownLatch;
        private KafkaConsumer<String, String> consumer;

        public ConsumerThread(CountDownLatch countDownLatch, KafkaConsumer<String, String> consumer){
            this.countDownLatch = countDownLatch;
            this.consumer = consumer;
        }

        @Override
        public void run(){


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

        public void shutdown(){
            consumer.wakeup();

        }
    }
}

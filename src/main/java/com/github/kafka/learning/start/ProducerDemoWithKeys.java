package com.github.kafka.learning.start;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerDemoWithKeys {
      static Logger logger = LoggerFactory.getLogger(ProducerDemoWithKeys.class);

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        // create producer properties

        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // create producer
        KafkaProducer<String, String> producer = new KafkaProducer(properties);

        for(int i=0; i < 10; i++) {
            //create ProducerRecord
            ProducerRecord<String, String> record = new ProducerRecord("first_topic", "Key "+ i, "Hello from Java " + i);
            //send data  async
            logger.info("KEY ID: " + i);
            producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (e == null) {
                        logger.info("Topic Name: {}", recordMetadata.topic());
                        logger.info("Partition No: {}", recordMetadata.partition());
                        logger.info("Offset No: {}", recordMetadata.offset());
                        logger.info("Key Size: {}", recordMetadata.serializedKeySize());
                        logger.info("Value Size: {}", recordMetadata.serializedValueSize());
                    } else {
                        logger.error("Error sending message");
                    }
                }
            }).get();  // It is synchronous blocking call. Don't do it in production
            //flush data
            producer.flush();
        }

        //flush and close
        producer.close();
    }
}

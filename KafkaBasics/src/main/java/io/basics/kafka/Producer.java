package io.basics.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import java.util.Properties;

public class Producer {

    private static final Logger log = LoggerFactory.getLogger(Producer.class.getSimpleName());
    public static void main(String[] args) {
        log.info("hello");

        //create Producer properties
        Properties properties = new Properties();
        properties.setProperty("key","value");

        //connecting to local host
        properties.setProperty("bootstrap.server", "127.0.0.1:8080");

        properties.setProperty("bootstrap.servers", "localhost:8080");

        //set producer properties
        properties.setProperty("key.serializer", "StringSerializer.class.getName()");
        properties.setProperty("value.serializer", "StringSerializer.class.getName()");

        //create the Producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        //create a Producer Record
        ProducerRecord<String, String> producerRecord =
                new ProducerRecord<>("First_topic","Helloo");

        //send data
        producer.send(producerRecord);

        //tell the producer to send all data and block until done -- synchronous
        producer.flush();

        //flush and close the producer
        producer.close();
    }
}


        // Create Kafka producer instance with the configured properties
        //KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        // Use the producer to send messages to Kafka topics
        // producer.send(new ProducerRecord<>("my-topic", "key", "value"));

        // Close the producer when done
        //producer.close();



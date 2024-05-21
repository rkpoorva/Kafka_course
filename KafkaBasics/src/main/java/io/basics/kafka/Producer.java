package io.basics.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import java.util.Properties;

public class Producer {

    private static final Logger log = LoggerFactory.getLogger(Producer.class.getSimpleName());
    public static void main(String[] args) {
        log.info("hello");

        //create Producer properties//
        Properties properties = new Properties();
        properties.setProperty("key","value");

        //connecting to local host server//
        properties.setProperty("bootstrap.server", "127.0.0.1:8080");
        //OR
        properties.setProperty("bootstrap.servers", "localhost:8080");
        //OR
        String bootstrapServers = "127.0.0.1:8080";
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        //how to connect to remote server//
        //properties.setProperty("security.protocol","SASL_SSL");
        // properties.setProperty("sasl.jaas.config","org.apache.kafka.common.security.plain.PlainLoginModule required username=\" \" password=\" \";);
        //properties.setProperty("sasl.mechanism","PLAIN");

        //set producer properties//
        // in order to specify how producer is going to behave
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", "StringSerializer.class.getName()");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        //StringSerializer.class.getName() - means that our producer is expecting strings which will be
        //serialized in bytes by key.serializer and value.serializer using StringSerializer class provided by kafka client

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




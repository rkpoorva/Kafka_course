package io.basics.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class Consumer {
    private static final Logger log = LoggerFactory.getLogger(Producer.class.getSimpleName());
    public static void main(String[] args) {
        log.info("Kafka Consumer");

        String groupId = "my-java-application";
        String topic = "Demo_java";

        //create Producer properties//
        Properties properties = new Properties();

        //connecting to local host server//
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");

        //Consumer configs
        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer", StringDeserializer.class.getName());

        properties.setProperty("group.id", groupId);
        properties.setProperty("auto.offset.reset", "earliest");
        //properties.setProperty("auto.offset.reset", "none/earliest/latest");
        //none means if we dont have any existing consumer group, then we fail that means that we must set the consumer group before starting the application
        //earliest means read from the beginning of the topic. this corresponds to the minus minus from beginning option when we looked at the kafka cli
        // we will choose earliest cuz we wanna read entire history of our topic
        //latest means " hey i want to read it from just now and only read the new messages sent from now

        //create consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        //subscribe to a topic
        consumer.subscribe(Arrays.asList(topic)); //we can pass collection of topics

        //poll for data - consumers poll data from kafka
        while(true){
            log.info("polling");

            ConsumerRecords<String,String> records = consumer.poll(Duration.ofMillis(1000));
            //now we need to extract the consumer and the duration is how long we are willing to wait to receive data
            //if kafka doesn't have any data, then we have to wait 1sec to receive data from kafka - with this we are not overloading the kafka
            //this returns a collection of records

            for(ConsumerRecord<String,String> record: records){
                log.info("Key: " + record.key() + ", Value: " + record.value());
                log.info("Partition: " + record.partition() + ", Value: " + record.value());
            }

        }

    }
}

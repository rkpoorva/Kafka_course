package io.basics.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerWithShutdown {
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

        //create consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        //get a reference to the main thread
        final Thread mainThread = Thread.currentThread(); //this is reference to the thread that is running the program

        //adding the shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(){
            public void run(){
                log.info("Detected a shutdown, lets exit by calling consumer.wakeup()...");

                consumer.wakeup(); //next time we will do consumer.poll in our code, that will throw a wakeup exception
                //once we allow and tell the consumer that it should throw an exception next time, so some code is going to execute after the while loop

                //now we need to make sure that shutdown hook is now waiting for the main program to finish
                //join the main thread to allow the execution of the code in the main method
                try {
                    mainThread.join();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });

        try{
            //subscribe to a topic
            consumer.subscribe(Arrays.asList(topic));

            //poll for data - consumers poll data from kafka
            while(true){
                //log.info("polling");

                ConsumerRecords<String,String> records = consumer.poll(Duration.ofMillis(1000));
                //now we need to extract the consumer and the duration is how long we are willing to wait to receive data
                //if kafka doesn't have any data, then we have to wait 1sec to receive data from kafka - with this we are not overloading the kafka
                //this returns a collection of records

                for(ConsumerRecord<String,String> record: records){
                    log.info("Key: " + record.key() + ", Value: " + record.value());
                    log.info("Partition: " + record.partition() + ", Value: " + record.value());
                }

            }
        } catch (WakeupException e){
            log.info("Consumer is starting to shut down");
        } catch(Exception e) {
            log.error("Unexpected exception in the consumer", e);
        } finally{
            consumer.close(); //close the consumer, this will also commit the offsets
            log.info("The consumer is now gracefully shut down");
        }
    }
}

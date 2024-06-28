package io.basics.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerKeys {
    private static final Logger log = LoggerFactory.getLogger(ProducerWithCallback.class.getSimpleName());
    public static void main(String[] args) {
        log.info("Kafka producer with api callbacks");

        //create Producer properties//
        Properties properties = new Properties();
        properties.setProperty("key","value");

        //connecting to local host server//
        properties.setProperty("bootstrap.server", "127.0.0.1:9092");

        //set producer properties//
        // in order to specify how producer is going to behave
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", "StringSerializer.class.getName()");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //create the Producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        //for loop for producing multiple messages at a time in our topic
        for(int i =0; i<30; i++){
            //create a Producer Record
            ProducerRecord<String, String> producerRecord = new ProducerRecord<>("First_topic","Helloo" + i );

            //send data
            producer.send(producerRecord, new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception e) {
                    //executes everytime a record successfully sent or an exception is thrown
                    if(e == null){
                        //the record is successfully sent
                        log.info("Received new metadata \n" +
                                  "Topic: " + metadata.topic() + "\n" +
                                   "Partition: " + metadata.partition() + "\n" +
                                    "Offset: " + metadata.offset() + "\n" +
                                    "Timestamp: " + metadata.timestamp() + "\n"
                            );
                    } else{
                            log.error("Error while producing", e);
                        }
                }
            });
        }

        //tell the producer to send all data and block until done -- synchronous
        producer.flush();

        //flush and close the producer
        producer.close();
    }
}

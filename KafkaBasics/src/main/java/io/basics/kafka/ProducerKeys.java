package io.basics.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerKeys {
    private static final Logger log = LoggerFactory.getLogger(ProducerKeys.class.getSimpleName());
    public static void main(String[] args) {
        log.info("Kafka producer with keys");

        //create Producer properties//
        Properties properties = new Properties();

        //connecting to local host server//
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");
        //properties.setProperty("bootstrap.servers", "localhost:9092");

        //set producer properties//
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

        //create the Producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        for(int j=0; j<2; j++){

            //for loop for producing multiple messages at a time in our topic
            for(int i =0; i<10; i++){

                String topic = "Demo_java";
                String key = "id_" + i;
                String value = "Helloo" + i;

                //create a Producer Record
                ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, key, value);

                //send data
                producer.send(producerRecord, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata metadata, Exception e) {
                        //executes everytime a record successfully sent or an exception is thrown
                        if(e == null){
                            //the record is successfully sent
                            log.info("Key: " + key + " | Partition: " + metadata.partition());
                        } else{
                            log.error("Error while producing", e);
                        }
                    }
                });
            }
        }

        //tell the producer to send all data and block until done -- synchronous
        producer.flush();

        //flush and close the producer
        producer.close();
    }
}

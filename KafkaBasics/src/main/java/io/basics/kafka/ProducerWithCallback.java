package io.basics.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerWithCallback {
    private static final Logger log = LoggerFactory.getLogger(ProducerWithCallback.class.getSimpleName());
    public static void main(String[] args) {
        log.info("Kafka producer with api callbacks");

        //create Producer properties//
        Properties properties = new Properties();
        properties.setProperty("key","value");

        //connecting to local host server//
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");
        //OR
        properties.setProperty("bootstrap.servers", "localhost:9092");
        //OR
        String bootstrapServers = "127.0.0.1:9092";
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

        properties.setProperty("batch.size", "400");
        //we would never go for a smaller batch size in production,by Kafka default its 16 kB of batch size.

        //this will make messages go to different partitions for each batch
        properties.setProperty("partitioner.class", RoundRobinPartitioner.class.getName());

        //create the Producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        //for loop if we need to switch between different partitions
        for(int j=0; j<10; j++){

            //for loop for producing multiple messages at a time in our topic
            for(int i =0; i<30; i++){
                //create a Producer Record
                ProducerRecord<String, String> producerRecord =
                        new ProducerRecord<>("First_topic","Helloo" + i );

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

            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

        }

        //tell the producer to send all data and block until done -- synchronous
        producer.flush();

        //flush and close the producer
        producer.close();
    }
}

package com.kafka.client;

import java.util.Date;
import java.util.Properties;
import java.util.Random;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class ProducerClientMain {
    public static void main(String[] args) {
        long events = Long.parseLong("10");
        Random rnd = new Random();
 
        Properties props = new Properties();
        props.put("metadata.broker.list", "localhost:9092");
        //The second property “serializer.class” defines what Serializer to use when preparing the message for transmission to the Broker. In our example we use a simple String encoder provided as part of Kafka. Note that the encoder must accept the same type as defined in the KeyedMessage object in the next step.
        //It is possible to change the Serializer for the Key (see below) of the message by defining "key.serializer.class" appropriately. By default it is set to the same value as "serializer.class".
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        //The third property  "partitioner.class" defines what class to use to determine which Partition in the Topic the message is to be sent to. 
        props.put("partitioner.class", "com.kafka.client.SimplePartitioner");
        props.put("request.required.acks", "1");
 
        ProducerConfig config = new ProducerConfig(props);
 
        //Note that the Producer is a Java Generic and you need to tell it the type of two parameters.
        //The first is the type of the Partition key, the second the type of the message.
        Producer<String, String> producer = new Producer<String, String>(config);
 
        for (long nEvents = 0; nEvents < events; nEvents++) { 
               long runtime = new Date().getTime();  
               String ip = "192.168.2." + rnd.nextInt(255); 
               String msg = runtime + ",www.example.com," + ip; 
               //Here we are passing IP as partition key
               //Note that if you do not include a key, even if you've defined a partitioner class, Kafka will assign the message to a random partition.
               //Other than this, every message should not go to same partiton . In my other code i am creating Message class object which is also having key inside, refer MessageProducer in local code. 
               KeyedMessage<String, String> data = new KeyedMessage<String, String>("LightingTestTopic", ip, msg);
               //Make sure you have LightingTestTopic topic , and with multiplr partition
               //kafka-topics --create --topic LightingTestTopic --replication-factor 1 --zookeeper localhost:2181 --partition 5  . Hence you will have 5 partitions like LightingTestTopic0 ...LightingTestTopic-4
               //If you have one broker then replication-factor would be 1.
               producer.send(data);
        }
        producer.close();
    }
}

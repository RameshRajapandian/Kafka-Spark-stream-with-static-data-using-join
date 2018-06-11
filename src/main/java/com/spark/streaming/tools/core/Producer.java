package com.spark.streaming.tools.core;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;


public class Producer {
   
    public static void main(String[] argv)throws Exception {
           String topicName = "test";
             //Configure the Producer
        Properties configProperties = new Properties();
        configProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        configProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.connect.json.JsonSerializer");
        configProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.connect.json.JsonSerializer");

        org.apache.kafka.clients.producer.Producer producer = new KafkaProducer(configProperties);

        ObjectMapper objectMapper = new ObjectMapper();

     
            Contact contact = new Contact();
            contact.parseString("2,asdjhajdhjas,sjlkasdjhasjh");
            JsonNode  jsonNode = objectMapper.valueToTree(contact);
            ProducerRecord<String, JsonNode> rec = new ProducerRecord<String, JsonNode>(topicName,jsonNode);
            producer.send(rec);
            producer.close();
        }
        
   
    }

package ru.spark.exchange.consume;

import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

@Slf4j
public class Producer {
    private final KafkaProducer kafkaProducer;
    {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("key.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
        kafkaProducer = new KafkaProducer(properties);
    }

    public void send(KafkaTopic topic, byte[] message) {
        val topicName = topic.getTopicName();
        val producerRecord = new ProducerRecord<>(topicName, message);
        kafkaProducer.send(producerRecord);
//        kafkaProducer.flush();
    }
}

package ru.spark.exchange;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

@Slf4j
public class Producer {
    private final KafkaProducer kafkaProducer;
    {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProducer = new KafkaProducer(properties);
    }

    public void send(KafkaTopic topic, String message) {
        String topicName = topic.getTopicName();
        log.info("В топик {} отправляется сообщение {}", topicName, message);
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topicName, message);
        kafkaProducer.send(producerRecord);
        kafkaProducer.flush();
    }
}

package ru.spark.exchange;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import ru.spark.exchange.consume.KafkaTopic;
import scala.Tuple2;

import java.util.*;

@Slf4j
public class SparkLauncherQuartzJob implements Job {
    @SneakyThrows
    @Override
    public void execute(JobExecutionContext jobExecutionContext) {
        SparkConf sparkConf = new SparkConf();
        sparkConf.setAppName("WordCountingApp");
        sparkConf.set("spark.cassandra.connection.host", "127.0.0.1");

        JavaStreamingContext streamingContext = new JavaStreamingContext(
                sparkConf, Durations.seconds(1));

        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", "localhost:9092");
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", "exchange-spark");
        kafkaParams.put("auto.offset.reset", "earliest");
        kafkaParams.put("enable.auto.commit", false);
        Collection<String> topics = Collections.singletonList(KafkaTopic.ORDER.getTopicName());

        JavaInputDStream<ConsumerRecord<String, String>> messages =
                KafkaUtils.createDirectStream(
                        streamingContext,
                        LocationStrategies.PreferConsistent(),
                        ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams));

        JavaPairDStream<String, String> results = messages
                .mapToPair(
                        record -> new Tuple2<>(record.key(), record.value())
                );
        JavaDStream<String> lines = results
                .map(
                        Tuple2::_2
                );
        JavaDStream<String> words = lines
                .flatMap(
                        x -> Arrays.asList(x.split("\\s+")).iterator()
                );
        JavaPairDStream<String, Integer> wordCounts = words
                .mapToPair(
                        s -> new Tuple2<>(s, 1)
                ).reduceByKey(
                        Integer::sum
                );

        wordCounts.foreachRDD(
                javaRdd -> {
                    Map<String, Integer> wordCountMap = javaRdd.collectAsMap();
                    for (String key : wordCountMap.keySet()) {
                        List<Word> wordList = Arrays.asList(new Word(key, wordCountMap.get(key)));
                        JavaRDD<Word> rdd = streamingContext.sparkContext().parallelize(wordList);
                        javaFunctions(rdd).writerBuilder(
                                "vocabulary", "words", mapToRow(Word.class)).saveToCassandra();
                    }
                }
        );
        streamingContext.start();
    }
}

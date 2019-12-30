package ru.spark.exchange;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
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
//todo org.apache.spark.SparkException: Only one SparkContext may be running in this JVM (see SPARK-2243). To ignore this error, set spark.driver.allowMultipleContexts = true. (ошибка при запуске пайплайна в кроне)
//todo
        log.info("execute job");
        SparkConf sparkConf = new SparkConf();
//        todo ???
        sparkConf.setMaster("local[2]");
        sparkConf.setAppName("ExchangeMonitoringApp");
//        sparkConf.set("spark.cassandra.connection.host", "127.0.0.1");

        JavaStreamingContext streamingContext = new JavaStreamingContext(
                sparkConf, Durations.seconds(1));

        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", "localhost:9092");
        kafkaParams.put("key.deserializer", ByteArrayDeserializer.class);
        kafkaParams.put("value.deserializer", ByteArrayDeserializer.class);
        kafkaParams.put("group.id", "exchange-spark");
        kafkaParams.put("auto.offset.reset", "earliest");
        kafkaParams.put("enable.auto.commit", false);
        Collection<String> topics = Collections.singletonList(KafkaTopic.ORDER.getTopicName());

        JavaInputDStream<ConsumerRecord<String, String>> messages =
                KafkaUtils.createDirectStream(
                        streamingContext,
                        LocationStrategies.PreferConsistent(),
                        ConsumerStrategies.Subscribe(topics, kafkaParams));

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
        wordCounts.print();
//        wordCounts.foreachRDD(
//                javaRdd -> {
//                    Map<String, Integer> wordCountMap = javaRdd.collectAsMap();
//                    for (String key : wordCountMap.keySet()) {
//                        log.info("key: {} value: {}", key, wordCountMap.get(key));
//                        List<Word> wordList = Arrays.asList(new Word(key, wordCountMap.get(key)));
//                        JavaRDD<Word> rdd = streamingContext.sparkContext().parallelize(wordList);
//                        javaFunctions(rdd).writerBuilder(
//                                "vocabulary", "words", mapToRow(Word.class)).saveToCassandra();
//                    }
//                }
//        );
//        sparkConf.
        streamingContext.start();
    }
}

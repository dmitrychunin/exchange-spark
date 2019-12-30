package ru.spark.exchange;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import ru.spark.exchange.consume.KafkaTopic;

import java.util.*;

//todo use Oozie instead quartz
@Slf4j
public class SparkLauncherQuartzJob implements Job {
    @SneakyThrows
    @Override
    public void execute(JobExecutionContext jobExecutionContext) {
//todo org.apache.spark.SparkException: Only one SparkContext may be running in this JVM (see SPARK-2243). To ignore this error, set spark.driver.allowMultipleContexts = true. (ошибка при запуске пайплайна в кроне)
//todo
        log.info("execute job");
//        sparkConf.set("spark.cassandra.connection.host", "127.0.0.1");
//        todo master???
//        2.12 streaming with kafka 0.10
//        SparkConf sparkConf = new SparkConf();
//        sparkConf.setMaster("local[2]");
//        sparkConf.setAppName("ExchangeMonitoringApp");
//
//        JavaStreamingContext streamingContext = new JavaStreamingContext(
//                sparkConf, Durations.seconds(1));
//        Map<String, Object> kafkaParams = new HashMap<>();
//        kafkaParams.put("bootstrap.servers", "localhost:9092");
//        kafkaParams.put("key.deserializer", ByteArrayDeserializer.class);
//        kafkaParams.put("value.deserializer", ByteArrayDeserializer.class);
//        kafkaParams.put("group.id", "exchange-spark");
//        kafkaParams.put("auto.offset.reset", "earliest");
//        kafkaParams.put("enable.auto.commit", false);
//        Collection<String> topics = Collections.singletonList(KafkaTopic.ORDER.getTopicName());
//        JavaInputDStream<ConsumerRecord<String, String>> messages =
//                KafkaUtils.createDirectStream(
//                        streamingContext,
//                        LocationStrategies.PreferConsistent(),
//                        ConsumerStrategies.Subscribe(topics, kafkaParams));
//        messages.print();

//        2.11 streaming with kafka 0.8
//        Map<String, String> kafkaParams = new HashMap<>();
//        kafkaParams.put("bootstrap.servers", "localhost:9092");
//        kafkaParams.put("key.deserializer", ByteArrayDeserializer.class.getName());
//        kafkaParams.put("value.deserializer", ByteArrayDeserializer.class.getName());
//        kafkaParams.put("group.id", "exchange-spark");
//        kafkaParams.put("auto.offset.reset", "smallest");
//        kafkaParams.put("enable.auto.commit", "false");
//        Set<String> topics = new HashSet<>(Collections.singletonList(KafkaTopic.ORDER.getTopicName()));
//
//        JavaPairInputDStream<String, String> messages = KafkaUtils.createDirectStream(
//                streamingContext,
//                String.class,
//                String.class,
//                StringDecoder.class,
//                StringDecoder.class,
//                kafkaParams,
//                topics);
//        messages.print();

//        structured streaming
//        SparkSession sparkSession = SparkSession.builder().master("local[2]").appName("ExchangeMonitoringApp").getOrCreate();
//        Dataset<Row> load = sparkSession.readStream()
//                .format("kafka")
//                .option("bootstrap.servers", "localhost:9092")
//                .option("key.deserializer", ByteArrayDeserializer.class.getName())
//                .option("value.deserializer", ByteArrayDeserializer.class.getName())
//                .option("group.id", "exchange-spark")
//                .option("auto.offset.reset", "earliest")
//                .option("enable.auto.commit", "false")
//                .option("subscribe", KafkaTopic.ORDER.getTopicName())
//                .load();
//
//        load.show();

//        streamingContext.start();
    }
}

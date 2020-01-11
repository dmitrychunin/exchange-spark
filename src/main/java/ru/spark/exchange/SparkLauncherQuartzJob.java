package ru.spark.exchange;

import kafka.serializer.StringDecoder;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import ru.spark.exchange.consume.KafkaTopic;

import java.util.*;

@Slf4j
public class SparkLauncherQuartzJob {//implements Job {
    private static SparkSession sparkSession;
    private static JavaStreamingContext streamingContext;

    static {
        SparkConf sparkConf = new SparkConf();
        sparkConf.setMaster("local[2]");
        sparkConf.setAppName("ExchangeMonitoringApp");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        streamingContext = new JavaStreamingContext(sc, Durations.seconds(1));
        sparkSession = SparkSession.builder()
                .config(sparkConf)
                .getOrCreate();

    }

    @SneakyThrows
//    @Override
    public static void execute(/*JobExecutionContext jobExecutionContext*/) {
        log.error("execute job");

        Map<String, String> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", "localhost:9092");
        kafkaParams.put("group.id", "exchange-spark");
        kafkaParams.put("auto.offset.reset", "smallest");
        kafkaParams.put("auto.commit.enable", "false");
        Set<String> topics = new HashSet<>(Collections.singletonList(KafkaTopic.ORDER.getTopicName()));

        JavaPairInputDStream<String, String> messages = KafkaUtils.createDirectStream(
                streamingContext,
                String.class,
                String.class,
                StringDecoder.class,
                StringDecoder.class,
                kafkaParams,
                topics);
//todo количество bids, asks, bids/asks
        messages.print();
        JavaDStream<String> map = messages.map(t -> t._2);
//        map.print();
        map
                .foreachRDD((VoidFunction<JavaRDD<String>>) rdd -> {

                    Dataset<Row> df = sparkSession.read().json(rdd);
//                    df.show();
//                    df.printSchema();

                    List<Row> b = df.select(
                            functions.size(new Column("b")).as("bids_count"),
                            functions.size(new Column("a")).as("asks_count")
                    ).collectAsList();

                    log.error("rowlist: {}", b.size());
                    int allBidsCount = 0;
                    int allAsksCount = 0;
                    for (Row row : b) {
                        int countOfBids = row.getAs("bids_count");
                        int countOfAsks = row.getAs("asks_count");
                        log.error("count of bids: {}", countOfBids);
                        log.error("count of asks: {}", countOfAsks);
                        allBidsCount += countOfBids;
                        allAsksCount += countOfAsks;
                    }
                    log.error("all count of bids: {}", allBidsCount);
                    log.error("all count of asks: {}", allAsksCount);
                    log.error("bids/asks: {}", allBidsCount/allAsksCount);
                });
        streamingContext.start();
    }
}

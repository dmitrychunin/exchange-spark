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
public class SparkLauncherQuartzJob implements Job {
    private static SparkSession sparkSession;
    private static JavaStreamingContext streamingContext;

    static {
        SparkConf sparkConf = new SparkConf();
        sparkConf.setMaster("local[2]");
        sparkConf.setAppName("ExchangeMonitoringApp");
        JavaSparkContext sc  = new JavaSparkContext(sparkConf);
        streamingContext = new JavaStreamingContext(sc, Durations.seconds(1));
        sparkSession = SparkSession.builder()
                .config(sparkConf)
                .getOrCreate();

    }

    @SneakyThrows
    @Override
    public void execute(JobExecutionContext jobExecutionContext) {
        log.info("execute job");

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
        map.print();
//        map
//                .foreachRDD((VoidFunction<JavaRDD<String>>) rdd -> {
//                    JavaRDD<Row> rowRDD = rdd.map(RowFactory::create);
//
//                    StructType schema = DataTypes.createStructType(new StructField[]{
//                            DataTypes.createStructField("e", DataTypes.StringType, false),
//                            DataTypes.createStructField("E", DataTypes.LongType, false),
//                            DataTypes.createStructField("s", DataTypes.StringType, false),
//                            DataTypes.createStructField("U", DataTypes.LongType, false),
//                            DataTypes.createStructField("u", DataTypes.LongType, false)//,
//                            DataTypes.createStructField("b", DataTypes.createArrayType(
//                                    DataTypes.createArrayType(
//                                            DataTypes.StringType, false
//                                    ),
//                                    false),
//                                    false),
//                            DataTypes.createStructField("a", DataTypes.createArrayType(
//                                    DataTypes.createArrayType(
//                                            DataTypes.StringType, false
//                                    ),
//                                    false),
//                                    false)
//
//                    });
//
//                    Dataset<Row> df = sparkSession.createDataFrame(rowRDD, schema);
//                    df.show();
//                    List<Row> b = df.select(functions.size(new Column("b"))).collectAsList();
//                    log.info("rowlist: {}", b);
//                    int countOfBids = b.get(0).getInt(0);
//
//                });
        streamingContext.start();
    }
}

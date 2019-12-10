package ru.spark.exchange;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.StreamingContext;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

public class SimpleApp {
  public static void main(String[] args) {
    SparkConf conf = new SparkConf();
    SparkContext sc = new SparkContext(conf);
    StreamingContext ssc = new StreamingContext(sc, new Duration(1000));
    JavaDStream<String> customReceiverStream = ssc.receiverStream(new JavaCustomReceiver(host, port));
    JavaDStream<String> words = lines.flatMap(new FlatMapFunction<String, String>() { ... });
    JavaStreamingContext

    String logFile = "YOUR_SPARK_HOME/README.md"; // Should be some file on your system
    SparkSession spark = SparkSession.builder().appName("Simple Application").getOrCreate();
    Dataset<String> logData = spark.read().textFile(logFile).cache();

    long numAs = logData.filter(s -> s.contains("a")).count();
    long numBs = logData.filter(s -> s.contains("b")).count();
    Spark
    System.out.println("Lines with a: " + numAs + ", lines with b: " + numBs);

    spark.stop();
  }
}
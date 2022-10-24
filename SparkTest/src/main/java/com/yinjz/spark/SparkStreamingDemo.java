package com.yinjz.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;


public class SparkStreamingDemo {
    public static void main(String[] args) throws Exception {
        SparkConf sparkConf = new SparkConf().setAppName("wc").setMaster("local[*]");
        JavaSparkContext jsc = new JavaSparkContext(sparkConf);
        JavaStreamingContext ssc = new JavaStreamingContext(jsc, Durations.seconds(5));
        jsc.setLogLevel("WARN");

//        JavaReceiverInputDStream<String> socketDStream = ssc.socketTextStream("localhost",9999);
//        JavaPairDStream<String, Integer> result = socketDStream
//                .flatMap(line -> Arrays.asList(line.split(" ")).iterator())
//                .mapToPair(word -> new Tuple2<>(word, 1))
//                .reduceByKey((a, b) -> a + b);
//
//        result.print();

        ssc.start();
        ssc.awaitTermination();
    }
}
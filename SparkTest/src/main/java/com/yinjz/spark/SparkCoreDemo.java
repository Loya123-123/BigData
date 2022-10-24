package com.yinjz.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;


public class SparkCoreDemo {
    public static void main(String[] args) throws InterruptedException {
        SparkConf sparkConf = new SparkConf()
                .setAppName("WC")
                .setMaster("local[*]");
        
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
        javaSparkContext.setLogLevel("INFO");

        JavaRDD<String> stringJavaRDD = javaSparkContext.textFile("data/input/words.txt");
        //通过查看源码,我们发现,flatMap中需要的函数的参数是T(就是String)
        //返回值是Iterator
        //所以我们在函数体里面要返回Iterator
//        JavaRDD<Object> wordRDD = stringJavaRDD.flatMap(line -> Arrays.asList(line.split(" ")).iterator());
//        //3.2每个单词记为1 (word,1)
//        JavaPairRDD<String, Integer> wordAndOneRDD = wordRDD.mapToPair(words -> new Tuple2<>(words, 1));
//        //3.3按照key进行聚合
//        JavaPairRDD<String, Integer> wordAndCountRDD = wordAndOneRDD.reduceByKey((a, b) -> a + b);
//        //4.收集结果并输出
//        List<Tuple2<String, Integer>> result = wordAndCountRDD.collect();
//        result.forEach(t -> System.out.println(t));

        Thread time;
        Thread.sleep(100000);
        javaSparkContext.stop();


    }
}

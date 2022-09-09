package com.yinjz.spark;


import lombok.val;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

import java.util.stream.Collectors;

public class SparkCoreTest {
    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession
                .builder()
                .appName("spark")
                .master("local[*]")
                .getOrCreate();

        Dataset<String> ds =  sparkSession.read().textFile("data/input/words.txt");
        val stringJavaRDD = ds.toJavaRDD();
        val map = stringJavaRDD.map(str -> str.split(" "));
//        ds.toDF("name").map(new MapFunction<String, String[]>(){
//            @Override
//            public String[] call(String value) throws Exception {
//                return value.split(" ");
//            }
//        }).show();
//        val rowDS: Dataset[(Int, Int)] = fileDS.map(line => {
//                val arr: Array[String] = line.split("\t")
//        (arr(1).toInt, arr(2).toInt)
//    })


    }



}

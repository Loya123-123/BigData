package com.yinjz.spark;


import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

public class SparkCoreTest {
    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession
                .builder()
                .appName("spark")
                .master("local[*]")
                .getOrCreate();

        Dataset<String> ds =  sparkSession.read().textFile("data/input/words.txt");
        JavaRDD<String> stringJavaRDD = ds.toJavaRDD();
        /*JavaRDD<String[]> map = stringJavaRDD.map(new MapFunction<String, String[]>(){


            @Override
            public String[] call(String value) throws Exception {
                return value.split(" ");
            }
        });*/
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

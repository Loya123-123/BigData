package com.yinjz.spark;

import org.apache.spark.SparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;

/**
 * Author itcast
 */
public class StructuredStreamingDemo {
    public static void main(String[] args) throws Exception {
        SparkSession spark = SparkSession
                .builder()
                .appName("SparkSQL")
                .master("local[*]")
                .config("spark.sql.shuffle.partitions", "4")
                .getOrCreate();
        SparkContext sc = spark.sparkContext();
        sc.setLogLevel("WARN");

        Dataset<Row> lines = spark
                .readStream()
                .format("socket")
                .option("host", "localhost")
                .option("port", 9999)
                .load();

        Dataset<Row> result = lines.as(Encoders.STRING())
                .flatMap((String line) -> Arrays.asList(line.split(" ")).iterator(), Encoders.STRING())
                .groupBy("value")
                .count();

        result.writeStream()
                .outputMode("complete")
                .format("console")
                .start()
                .awaitTermination();
    }
}
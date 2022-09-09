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
public class SparkSQLDemo {
    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .appName("SparkSQL")
                .master("local[*]")
                .getOrCreate();
        SparkContext sc = spark.sparkContext();
        sc.setLogLevel("INFO");

        Dataset<String> ds = spark.read().textFile("data/input/words.txt");
        ds.printSchema();
        ds.show();

        Dataset<String> wordDS = ds.flatMap((String line) -> Arrays.asList(line.split(" ")).iterator(), Encoders.STRING());
        wordDS.printSchema();
        wordDS.show();

        //SQL
        wordDS.createOrReplaceTempView("t_words");
        String sql = "select value as word,count(*) as counts\n" +
                "        from t_words\n" +
                "        group by word\n" +
                "        order by counts desc";
        spark.sql(sql).show(false);

        //DSL
        Dataset<Row> tempDS = wordDS.groupBy("value")
                .count();//.show(false);
        tempDS.orderBy(tempDS.col("count").desc())
                .show(false);

    }
}
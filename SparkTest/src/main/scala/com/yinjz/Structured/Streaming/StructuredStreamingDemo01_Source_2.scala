package com.yinjz.Structured.Streaming

import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/**
 * Author itcast
 * Desc 演示 StructuredStreaming入门案例-从RateSource
 */
object StructuredStreamingDemo01_Source_2 {
  def main(args: Array[String]): Unit = {
    //1.创建环境
    val spark: SparkSession = SparkSession.builder().appName("StructuredStreaming").master("local[*]")
      .config("spark.sql.shuffle.partitions", "2")
      .getOrCreate()
    val sc: SparkContext = spark.sparkContext
    sc.setLogLevel("WARN")

    //2.source
    //使用RateSource每秒生成10条测试数据--学习测试时使用
    val rateStreamDF: DataFrame = spark.readStream
      .format("rate")
      .option("rowsPerSecond", "10") //每秒生成数据条数
      .option("rampUpTime", "0s") //每条数据生成间隔时间
      .option("numPartitions", "2") //分区数目
      .load()
    rateStreamDF.printSchema()
    /*root
       |-- timestamp: timestamp (nullable = true)
       |-- value: long (nullable = true)
     */

    //3.输出
    rateStreamDF
      .writeStream
      .outputMode("append") //输出模式
      .format("console")
      .option("truncate", false)
      //5.启动并等待停止
      .start()
      .awaitTermination()
  }
}
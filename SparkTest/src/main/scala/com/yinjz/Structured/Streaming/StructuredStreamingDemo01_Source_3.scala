package com.yinjz.Structured.Streaming

import org.apache.spark.SparkContext
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Author itcast
 * Desc 演示 StructuredStreaming入门案例-从FileSource
 */
object StructuredStreamingDemo01_Source_3 {
  def main(args: Array[String]): Unit = {
    //1.创建环境
    val spark: SparkSession = SparkSession.builder().appName("StructuredStreaming").master("local[*]")
      .config("spark.sql.shuffle.partitions", "2")
      .getOrCreate()
    val sc: SparkContext = spark.sparkContext
    sc.setLogLevel("WARN")

    //2.source
    val csvSchema: StructType = new StructType()
      .add("name", StringType, nullable = true)
      .add("age", IntegerType, nullable = true)
      .add("hobby", StringType, nullable = true)

    val fileDF: DataFrame = spark.readStream
      .option("sep", ";")
      .option("header", "false")
      .schema(csvSchema)
      .csv("data/input/persons")// Equivalent to format("csv").load("/path/to/directory")

    //3.输出
    fileDF
      .writeStream
      .outputMode("update")
      .format("console")
      .option("truncate",false)
      .start()
      .awaitTermination()
  }
}
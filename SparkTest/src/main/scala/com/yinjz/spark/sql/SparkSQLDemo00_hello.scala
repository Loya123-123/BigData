package com.yinjz.spark.sql

import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/**
 * Author itcast
 * Desc 演示SparkSQL
 */
object SparkSQLDemo00_hello {
  def main(args: Array[String]): Unit = {
    //1.准备SparkSQL开发环境
    println(this.getClass.getSimpleName)
    println(this.getClass.getSimpleName.stripSuffix("$"))
    val spark: SparkSession = SparkSession.builder().appName(this.getClass.getSimpleName.stripSuffix("$")).master("local[*]").getOrCreate()
    val sc: SparkContext = spark.sparkContext
    sc.setLogLevel("WARN")

    val df1: DataFrame = spark.read.text("data/input/text")
    val df2: DataFrame = spark.read.json("data/input/json")
    val df3: DataFrame = spark.read.csv("data/input/csv")
    val df4: DataFrame = spark.read.parquet("data/input/parquet")

    df1.printSchema()
    df1.show(false)
    df2.printSchema()
    df2.show(false)
    df3.printSchema()
    df3.show(false)
    df4.printSchema()
    df4.show(false)


    df1.coalesce(1).write.mode(SaveMode.Overwrite).text("data/output/text")
    df2.coalesce(1).write.mode(SaveMode.Overwrite).json("data/output/json")
    df3.coalesce(1).write.mode(SaveMode.Overwrite).csv("data/output/csv")
    df4.coalesce(1).write.mode(SaveMode.Overwrite).parquet("data/output/parquet")

    //关闭资源
    sc.stop()
    spark.stop()
  }
}
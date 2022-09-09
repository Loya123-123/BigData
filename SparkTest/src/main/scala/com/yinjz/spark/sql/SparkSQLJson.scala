package com.yinjz.spark.sql

import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
 * SparkSQL读取JSON格式文本数据
 */
object SparkSQLJson {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName(this.getClass.getSimpleName.stripSuffix("$"))
      .master("local[*]")
      // 通过装饰模式获取实例对象，此种方式为线程安全的
      .getOrCreate()
    val sc: SparkContext = spark.sparkContext
    sc.setLogLevel("WARN")
    import spark.implicits._

    // TODO: 从LocalFS上读取json格式数据(压缩）
    val jsonDF: DataFrame = spark.read.json("data/input/2015-03-01-11.json.gz")
    //jsonDF.printSchema()
    jsonDF.show(5, truncate = true)

    println("===================================================")
    val githubDS: Dataset[String] = spark.read.textFile("data/input/2015-03-01-11.json.gz")
    //githubDS.printSchema() // value 字段名称，类型就是String
    githubDS.show(5,truncate = true)

    // TODO：使用SparkSQL自带函数，针对JSON格式数据解析的函数
    import org.apache.spark.sql.functions._
    // 获取如下四个字段的值：id、type、public和created_at
    val gitDF: DataFrame = githubDS.select(
      get_json_object($"value", "$.id").as("id"),
      get_json_object($"value", "$.type").as("type"),
      get_json_object($"value", "$.public").as("public"),
      get_json_object($"value", "$.created_at").as("created_at")
    )
    gitDF.printSchema()
    gitDF.show(10, truncate = false)

    // 应用结束，关闭资源
    spark.stop()
  }
}
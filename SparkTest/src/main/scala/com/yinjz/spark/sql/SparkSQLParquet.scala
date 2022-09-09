package com.yinjz.spark.sql

import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * SparkSQL读取Parquet列式存储数据
 */
object SparkSQLParquet {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName(this.getClass.getSimpleName.stripSuffix("$"))
      .master("local[*]")
      // 通过装饰模式获取实例对象，此种方式为线程安全的
      .getOrCreate()
    val sc: SparkContext = spark.sparkContext
    sc.setLogLevel("WARN")
    import spark.implicits._

    // TODO: 从LocalFS上读取parquet格式数据
    val usersDF: DataFrame = spark.read.parquet("data/input/users.parquet")
    usersDF.printSchema()
    usersDF.show(10, truncate = false)

    println("==================================================")

    // SparkSQL默认读取文件格式为parquet
    val df = spark.read.load("data/input/users.parquet")
    df.printSchema()
    df.show(10, truncate = false)

    // 应用结束，关闭资源
    spark.stop()
  }
}
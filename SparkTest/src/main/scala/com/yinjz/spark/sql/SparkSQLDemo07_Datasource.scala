package com.yinjz.spark.sql

import java.util.Properties

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode, SparkSession}

/**
 * Author itcast
 * Desc 演示SparkSQL-外部数据源
 */
object SparkSQLDemo07_Datasource {
  def main(args: Array[String]): Unit = {
    //1.准备SparkSQL开发环境
    val spark: SparkSession = SparkSession.builder().appName("hello").master("local[*]").getOrCreate()
    val sc: SparkContext = spark.sparkContext
    sc.setLogLevel("WARN")
    import spark.implicits._

    //获取DF/DS
    //方式1:RDD-->DF/DS:兼容之前的RDD的项目
    //方式2:直接读取为DF/DS:优先考虑使用,支持多种数据源/数据格式:json/csv/parquet/jdbc....

    //需求:准备一个DF,写入到不同的数据格式/数据源中,然后再读出来
    //2.准备一个DF
    val fileRDD: RDD[String] = sc.textFile("data/input/person.txt")
    val personRDD: RDD[Person] = fileRDD.map(line => {
      val arr: Array[String] = line.split(" ")
      Person(arr(0).toInt, arr(1), arr(2).toInt)
    })
    import spark.implicits._ //隐式转换
    val df: DataFrame = personRDD.toDF()
    df.printSchema()
    df.show(false)



    //TODO 写
    //df.coalesce(1).write.mode(SaveMode.Overwrite)
    //.text("data/output/text")//注意:往普通文件写不支持Schema
    df.coalesce(1).write.mode(SaveMode.Overwrite).json("data/output/json")
    df.coalesce(1).write.mode(SaveMode.Overwrite).csv("data/output/csv")
    df.coalesce(1).write.mode(SaveMode.Overwrite).parquet("data/output/parquet")
    val prop = new Properties()
    prop.setProperty("user","root")
    prop.setProperty("password","root")
    df.coalesce(1).write.mode(SaveMode.Overwrite).jdbc("jdbc:mysql://localhost:3306/bigdata?characterEncoding=UTF-8","person",prop)//表会自动创建

    //TODO 读
    //spark.read.text("data/output/text").show(false)
    spark.read.json("data/output/json").show(false)
    spark.read.csv("data/output/csv").toDF("id1","name1","age1").show(false)
    spark.read.parquet("data/output/parquet").show(false)
    spark.read.jdbc("jdbc:mysql://localhost:3306/bigdata?characterEncoding=UTF-8","person",prop).show(false)

    sc.stop()
    spark.stop()
  }
  case class Person(id:Int,name:String,age:Int)
}
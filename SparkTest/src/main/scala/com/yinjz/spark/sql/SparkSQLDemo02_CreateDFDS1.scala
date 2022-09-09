package com.yinjz.spark.sql

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
 * Author itcast
 * Desc 演示SparkSQL-RDD-->DataFrame/DataSet使用样例类
 */
object SparkSQLDemo02_CreateDFDS1 {
  def main(args: Array[String]): Unit = {
    //1.准备SparkSQL开发环境
    //注意:在新版的Spark中,使用SparkSession来进行SparkSQL开发!
    //因为SparkSession封装了SqlContext、HiveContext、SparkContext
    val spark: SparkSession = SparkSession.builder().appName("hello").master("local[*]").getOrCreate()
    val sc: SparkContext = spark.sparkContext
    sc.setLogLevel("WARN")

    //2.获取RDD
    val fileRDD: RDD[String] = sc.textFile("data/input/person.txt")
    val personRDD: RDD[Person] = fileRDD.map(line => {
      val arr: Array[String] = line.split(" ")
      Person(arr(0).toInt, arr(1), arr(2).toInt)
    })
    //3.RDD->DataFrame/DataSet
    import spark.implicits._ //隐式转换
    val df: DataFrame = personRDD.toDF()
    val ds: Dataset[Person] = personRDD.toDS()

    //4.输出约束和类型
    df.printSchema()
    df.show()

    ds.printSchema()
    ds.show()

    //5.关闭资源
    sc.stop()
    spark.stop()
  }
  case class Person(id:Int,name:String,age:Int)
}
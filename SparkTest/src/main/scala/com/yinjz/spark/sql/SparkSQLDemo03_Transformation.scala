package com.yinjz.spark.sql

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/**
 * Author itcast
 * Desc 演示SparkSQL-RDD/DataFrame/DataSet相互转换
 */
object SparkSQLDemo03_Transformation {
  def main(args: Array[String]): Unit = {
    //1.准备SparkSQL开发环境
    val spark: SparkSession = SparkSession.builder().appName("hello").master("local[*]").getOrCreate()
    val sc: SparkContext = spark.sparkContext
    sc.setLogLevel("WARN")

    //2.获取RDD
    val fileRDD: RDD[String] = sc.textFile("data/input/person.txt")
    val personRDD: RDD[Person] = fileRDD.map(line => {
      val arr: Array[String] = line.split(" ")
      Person(arr(0).toInt, arr(1), arr(2).toInt)
    })

    import spark.implicits._ //隐式转换

    //3.相互转换
    // RDD->DF
    val df: DataFrame = personRDD.toDF()
    // RDD->DS
    val ds: Dataset[Person] = personRDD.toDS()
    // DF->RDD
    val rdd: RDD[Row] = df.rdd //注意:rdd->df的时候泛型丢了,所以df->rdd的时候就不知道原来的泛型了,给了个默认的
    // DF->DS
    val ds2: Dataset[Person] = df.as[Person] //给df添加上泛型
    // DS->RDD
    val rdd2: RDD[Person] = ds.rdd
    // DS->DF
    val df2: DataFrame = ds.toDF()

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
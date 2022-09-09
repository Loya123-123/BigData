package com.yinjz.spark.sql

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

/**
 * Author itcast
 * Desc 演示SparkSQL-RDD-->DataFrame/DataSet-自定义Schema
 */
object SparkSQLDemo02_CreateDFDS3 {
  def main(args: Array[String]): Unit = {
    //1.准备SparkSQL开发环境
    //注意:在新版的Spark中,使用SparkSession来进行SparkSQL开发!
    //因为SparkSession封装了SqlContext、HiveContext、SparkContext
    val spark: SparkSession = SparkSession.builder().appName("hello").master("local[*]").getOrCreate()
    val sc: SparkContext = spark.sparkContext
    sc.setLogLevel("WARN")

    //2.获取RDD
    val fileRDD: RDD[String] = sc.textFile("data/input/person.txt")
    //准备rowRDD:RDD[Row]
    val rowRDD: RDD[Row] = fileRDD.map(line => {
      val arr: Array[String] = line.split(" ")
      Row(arr(0).toInt, arr(1), arr(2).toInt)
    })

    //准备Schema
    /*val schema: StructType = StructType(
          StructField("id", IntegerType, true) ::
          StructField("name", StringType, true) ::
          StructField("age", IntegerType, true) :: Nil)*/
    val schema: StructType = StructType(
      List(
        StructField("id", IntegerType, true),
        StructField("name", StringType, true),
        StructField("age", IntegerType, true)
      )
    )

    //3.RDD->DataFrame/DataSet
    import spark.implicits._ //隐式转换
    val df: DataFrame = spark.createDataFrame(rowRDD, schema)


    //4.输出约束和类型
    df.printSchema()
    df.show()

    //5.关闭资源
    sc.stop()
    spark.stop()
  }
}
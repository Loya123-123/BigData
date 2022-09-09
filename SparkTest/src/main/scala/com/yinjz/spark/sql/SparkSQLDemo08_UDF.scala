package com.yinjz.spark.sql

import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Author itcast
 * Desc 演示SparkSQL-自定义UDF
 */
object SparkSQLDemo08_UDF {
  def main(args: Array[String]): Unit = {
    //1.准备SparkSQL开发环境
    val spark: SparkSession = SparkSession.builder().appName("hello").master("local[*]").getOrCreate()
    val sc: SparkContext = spark.sparkContext
    sc.setLogLevel("WARN")

    import org.apache.spark.sql.functions._
    import spark.implicits._

    //2.获取数据DF->DS->RDD
    val df: DataFrame = spark.read.text("data/input/udf.txt")
    df.printSchema()
    df.show(false)
    /*
    root
   |-- value: string (nullable = true)

  +-----+
  |value|
  +-----+
  |hello|
  |haha |
  |hehe |
  |xixi |
  +-----+
     */

    //TODO =======SQL风格=======
    //3.自定义UDF:String-->大写
    spark.udf.register("small2big",(value:String)=>{
      value.toUpperCase
    })

    //4.执行查询转换
    df.createOrReplaceTempView("t_words")
    val sql =
      """
        |select value,small2big(value) big_value
        |from t_words
        |""".stripMargin
    spark.sql(sql).show(false)

    //TODO =======DSL风格=======
    //3.自定义UDF:String-->大写


    //4.执行查询转换
    val small2big2 = udf((value:String)=>{
      value.toUpperCase
    })
    df.select('value,small2big2('value).as("big_value2")).show(false)


    //5.关闭资源
    sc.stop()
    spark.stop()
  }
}
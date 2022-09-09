package com.yinjz.spark.sql

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
 * Author itcast
 * Desc 演示SparkSQL-完成WordCount
 */
object SparkSQLDemo05_WordCount {
  def main(args: Array[String]): Unit = {
    //1.准备SparkSQL开发环境
    val spark: SparkSession = SparkSession.builder().appName("hello").master("local[*]").getOrCreate()
    val sc: SparkContext = spark.sparkContext
    sc.setLogLevel("WARN")
    import spark.implicits._

    //2.获取DF/DS
    //获取DF/DS方式一:通过RDD->DF/DS
    val fileRDD: RDD[String] = sc.textFile("data/input/words.txt")
    val df: DataFrame = fileRDD.toDF("value")
    val ds: Dataset[String] = df.as[String]
    df.printSchema()
    df.show(false)
    ds.printSchema()
    ds.show(false)

    //获取DF/DS方式二:
    val df2: DataFrame = spark.read.text("data/input/words.txt")
    df2.printSchema()
    df2.show(false)
    val ds2: Dataset[String] = spark.read.textFile("data/input/words.txt")
    ds2.printSchema()
    ds2.show(false)
    /*
    root
   |-- value: string (nullable = true)

  +----------------+
  |value           |
  +----------------+
  |hello me you her|
  |hello you her   |
  |hello her       |
  |hello           |
  +----------------+
     */

    //3.计算WordCount
    //df.flatMap(_.split(" ")) //报错:DF没有泛型,不知道_是String
    //df2.flatMap(_.split(" "))//报错:DF没有泛型,不知道_是String
    val wordDS: Dataset[String] = ds.flatMap(_.split(" "))
    //ds2.flatMap(_.split(" "))

    wordDS.printSchema()
    wordDS.show(false)
    /*
    +-----+
    |value|
    +-----+
    |hello|
    |me   |
    |you  |
    ....
     */

    //TODO SQL风格
    wordDS.createOrReplaceTempView("t_words")
    val sql: String =
      """
        |select value as word,count(*) as counts
        |from t_words
        |group by word
        |order by counts desc
        |""".stripMargin
    spark.sql(sql).show(false)

    //TODO DSL风格
    wordDS.groupBy("value")
      .count()
      .orderBy('count.desc)
      .show(false)
    /*
    +-----+------+
    |word |counts|
    +-----+------+
    |hello|4     |
    |her  |3     |
    |you  |2     |
    |me   |1     |
    +-----+------+

    +-----+-----+
    |value|count|
    +-----+-----+
    |hello|4    |
    |her  |3    |
    |you  |2    |
    |me   |1    |
    +-----+-----+
     */


    //4.关闭资源
    sc.stop()
    spark.stop()
  }

  case class Person(id: Int, name: String, age: Int)

}
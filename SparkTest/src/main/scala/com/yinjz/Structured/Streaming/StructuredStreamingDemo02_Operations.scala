package com.yinjz.Structured.Streaming

import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/**
 * Author itcast
 * Desc 演示 StructuredStreaming-Operations
 */
object StructuredStreamingDemo02_Operations{
  def main(args: Array[String]): Unit = {
    //1.创建环境
    val spark: SparkSession = SparkSession.builder().appName("StructuredStreaming").master("local[*]")
      .config("spark.sql.shuffle.partitions", "2")
      .getOrCreate()
    val sc: SparkContext = spark.sparkContext
    sc.setLogLevel("WARN")
    import spark.implicits._

    //2.source
    val linesDF: DataFrame = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 9999)
      .load()
    linesDF.printSchema()
    /*
    root
     |-- value: string (nullable = true)
     */

    //3.transformation-做WordCount
    val wordsDS: Dataset[String] = linesDF.as[String]
      .flatMap(_.split(" "))
    /*
    value
    单词
    单词
   ....
     */
    //TODO:DSL方式,以前怎么写现在就怎么写
    val resultDS: Dataset[Row] = wordsDS
      .groupBy("value")
      .count()
      .sort('count.desc)

    //TODO:SQL方式,以前怎么写现在就怎么写
    wordsDS.createOrReplaceTempView("t_words")
    val sql: String =
      """
        |select value as word,count(*) as counts
        |from t_words
        |group by word
        |order by counts desc
        |""".stripMargin
    val resultDS2: DataFrame = spark.sql(sql)

    //4.输出
    println(resultDS.isStreaming)
    resultDS
      .writeStream
      .outputMode("complete")
      .format("console")
      //5.启动并等待停止
      .start()
    //.awaitTermination()//注意:因为后面还有代码需要执行,所以这里的阻塞等待需要注掉

    println(resultDS2.isStreaming)
    resultDS2
      .writeStream
      .outputMode("complete")
      .format("console") //往控制台输出
      //5.启动并等待停止
      .start()
      .awaitTermination()
  }
}
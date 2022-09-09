package com.yinjz.spark.sql

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.avg
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

// 对电影评分数据进行统计分析，分别使用DSL编程和SQL编程，获取电影平均分Top10，要求电影的评分次数大于200
object SparkSQLDemo06_MovieTop10 {
  def main(args: Array[String]): Unit = {
    // spark 环境
    val spark: SparkSession = SparkSession
      .builder()
      .appName("Sparksql")
      .config("spark.sql.shuffle.partitions", "4")
      .master("local[*]")
      .getOrCreate()
    val sc: SparkContext = spark.sparkContext
    sc.setLogLevel("WARN")
    import spark.implicits._

    // 2 获取df 、ds

    val fileDS: Dataset[String] = spark.read.textFile("data/input/rating_100k.data")
    val rowDS: Dataset[(Int, Int)] = fileDS.map(line => {
      val arr: Array[String] = line.split("\t")
      (arr(1).toInt, arr(2).toInt)
    })
    val cleanDF: DataFrame = rowDS.toDF("mid", "score")
    cleanDF.printSchema()
    cleanDF.show(false)

    /* +----+-----+
     |mid |score|
     +----+-----+
     |242 |3    |
     |302 |3    |
     |377 |1    |
     |51  |2    |
     |346 |1    |*/
    Thread.sleep(1000)
    // 获取电影平均分Top10，要求电影的评分次数大于200
    // TODO SQL
    cleanDF.createOrReplaceTempView("t_scores")
    val sql: String =
      """
        |select mid,round(avg(score),2) avg,count(*) counts
        |from t_scores
        |group by mid
        |having counts > 200
        |order by avg desc,counts desc
        |limit 10
        |""".stripMargin
    spark.sql(sql).show(false)
    Thread.sleep(1000)
    // TODO DSL
    import org.apache.spark.sql.functions._
    cleanDF
      .groupBy("mid")
      .agg(
        round(avg('score), 2) as "avg",
        count('mid) as "counts"
      ) //聚合函数可以写在这里
      .orderBy('avg.desc, 'counts.desc)
      .filter('counts > 200)
      .limit(10)
      .show(false)
    Thread.sleep(100000)
    // 关闭资源
    sc.stop()
    spark.stop()

  }

}

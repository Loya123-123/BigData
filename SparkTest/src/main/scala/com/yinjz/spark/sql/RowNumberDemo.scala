package com.yinjz.spark.sql

import org.apache.spark.SparkContext
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/**
 * Author itcast
 * Desc 使用SparkSQL支持的开窗函数/窗口函数完成对各个班级的学生成绩的排名
 */
object RowNumberDemo {
  case class Score(name: String, clazz: Int, score: Int)
  def main(args: Array[String]): Unit = {
    //1.准备环境
    val spark: SparkSession = SparkSession.builder().appName("WordCount").master("local[*]").getOrCreate()
    val sc: SparkContext = spark.sparkContext
    sc.setLogLevel("WARN")
    import spark.implicits._

    //2.加载数据
    val scoreDF: DataFrame = sc.makeRDD(Array(
      Score("a1", 1, 80),
      Score("a2", 1, 78),
      Score("a3", 1, 95),
      Score("a4", 2, 74),
      Score("a5", 2, 92),
      Score("a6", 3, 99),
      Score("a7", 3, 99),
      Score("a8", 3, 45),
      Score("a9", 3, 55),
      Score("a10", 3, 78),
      Score("a11", 3, 100))
    ).toDF("name", "class", "score")
    scoreDF.createOrReplaceTempView("t_scores")
    scoreDF.show()
    /*
    +----+-----+-----+
    |name|class|score|num
    +----+-----+-----+
    |  a1|    1|   80|
    |  a2|    1|   78|
    |  a3|    1|   95|
    |  a4|    2|   74|
    |  a5|    2|   92|
    |  a6|    3|   99|
    |  a7|    3|   99|
    |  a8|    3|   45|
    |  a9|    3|   55|
    | a10|    3|   78|
    | a11|    3|  100|
    +----+-----+-----+
     */


    //使用ROW_NUMBER顺序排序
    spark.sql("select name, class, score, row_number() over(partition by class order by score) num from t_scores").show()
    //使用RANK跳跃排序
    spark.sql("select name, class, score, rank() over(partition by class order by score) num from t_scores").show()
    //使用DENSE_RANK连续排序
    spark.sql("select name, class, score, dense_rank() over(partition by class order by score) num from t_scores").show()

    /*
ROW_NUMBER顺序排序--1234
+----+-----+-----+---+
|name|class|score|num|
+----+-----+-----+---+
|  a2|    1|   78|  1|
|  a1|    1|   80|  2|
|  a3|    1|   95|  3|
|  a8|    3|   45|  1|
|  a9|    3|   55|  2|

| a10|    3|   78|  3|
|  a6|    3|   99|  4|
|  a7|    3|   99|  5|
| a11|    3|  100|  6|

|  a4|    2|   74|  1|
|  a5|    2|   92|  2|
+----+-----+-----+---+

使用RANK跳跃排序--1224
+----+-----+-----+---+
|name|class|score|num|
+----+-----+-----+---+
|  a2|    1|   78|  1|
|  a1|    1|   80|  2|
|  a3|    1|   95|  3|
|  a8|    3|   45|  1|
|  a9|    3|   55|  2|

| a10|    3|   78|  3|
|  a6|    3|   99|  4|
|  a7|    3|   99|  4|
| a11|    3|  100|  6|

|  a4|    2|   74|  1|
|  a5|    2|   92|  2|
+----+-----+-----+---+

DENSE_RANK连续排序--1223
+----+-----+-----+---+
|name|class|score|num|
+----+-----+-----+---+
|  a2|    1|   78|  1|
|  a1|    1|   80|  2|
|  a3|    1|   95|  3|
|  a8|    3|   45|  1|
|  a9|    3|   55|  2|

| a10|    3|   78|  3|
|  a6|    3|   99|  4|
|  a7|    3|   99|  4|
| a11|    3|  100|  5|

|  a4|    2|   74|  1|
|  a5|    2|   92|  2|
+----+-----+-----+---+
     */

    /*

    val sql =
      """
        |select 字段1,字段2,字段n,
        |row_number() over(partition by 字段1 order by 字段2 desc) num
        |from 表名
        |having num <= 3
        |""".stripMargin

    import org.apache.spark.sql.functions._
    df.withColumn(
      "num",
      row_number().over(Window.partitionBy('字段1).orderBy('字段2.desc))
    ).filter('num <= 3).show(false)

     */
  }
}
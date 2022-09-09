package com.yinjz.spark.sql

import org.apache.spark.SparkContext
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/**
 * SparkSQL 读取CSV/TSV格式数据：
 * i). 指定Schema信息
 * ii). 是否有header设置
 */
object SparkSQLCsv {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName(this.getClass.getSimpleName.stripSuffix("$"))
      .master("local[*]")
      // 通过装饰模式获取实例对象，此种方式为线程安全的
      .getOrCreate()
    val sc: SparkContext = spark.sparkContext
    sc.setLogLevel("WARN")
    import spark.implicits._

    /**
     * 实际企业数据分析中
     * csv\tsv格式数据，每个文件的第一行（head, 首行），字段的名称（列名）
     */
    // TODO: 读取CSV格式数据
    val ratingsDF: DataFrame = spark.read
      // 设置每行数据各个字段之间的分隔符， 默认值为 逗号
      .option("sep", "\t")
      // 设置数据文件首行为列名称，默认值为 false
      .option("header", "true")
      // 自动推荐数据类型，默认值为false
      .option("inferSchema", "true")
      // 指定文件的路径
      .csv("data/input/rating_100k_with_head.data")

    ratingsDF.printSchema()
    ratingsDF.show(10, truncate = false)

    println("=======================================================")
    // 定义Schema信息
    val schema = StructType(
      StructField("user_id", IntegerType, nullable = true) ::
        StructField("movie_id", IntegerType, nullable = true) ::
        StructField("rating", DoubleType, nullable = true) ::
        StructField("timestamp", StringType, nullable = true) :: Nil
    )

    // TODO: 读取CSV格式数据
    val mlRatingsDF: DataFrame = spark.read
      // 设置每行数据各个字段之间的分隔符， 默认值为 逗号
      .option("sep", "\t")
      // 指定Schema信息
      .schema(schema)
      // 指定文件的路径
      .csv("data/input/rating_100k.data")

    mlRatingsDF.printSchema()
    mlRatingsDF.show(10, truncate = false)

    println("=======================================================")
    /**
     * 将电影评分数据保存为CSV格式数据
     */
    mlRatingsDF
      // 降低分区数，此处设置为1，将所有数据保存到一个文件中
      .coalesce(1)
      .write
      // 设置保存模式，依据实际业务场景选择，此处为覆写
      .mode(SaveMode.Overwrite)
      .option("sep", ",")
      // TODO: 建议设置首行为列名
      .option("header", "true")
      .csv("data/output/ml-csv-" + System.currentTimeMillis())

    // 关闭资源
    spark.stop()
  }

}
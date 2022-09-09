package com.yinjz.spark.sql

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

/**
 * Author itcast
 * Desc 演示SparkSQL-OnHive的元数据库(语法解析,物理执行计划生成,执行引擎,SQL优化都是用的Spark的)
 * 遇到权限问题暂时没有解决
 */
object SparkSQLDemo09_SparkOnHive {
  def main(args: Array[String]): Unit = {
    //1.准备SparkSQL开发环境
    val spark: SparkSession = SparkSession.builder().appName("SparkSQL").master("local[*]")
      .config("spark.sql.shuffle.partitions", "4")//默认是200,本地测试给少一点
      .config("spark.sql.warehouse.dir", "hdfs://node1:8020/user/hive/warehouse")//指定Hive数据库在HDFS上的位置
      .config("hive.metastore.uris", "thrift://node1:9083")
      .enableHiveSupport()//开启对hive语法的支持
      .getOrCreate()
    val sc: SparkContext = spark.sparkContext
    sc.setLogLevel("WARN")

    //2.执行Hive-SQL
    spark.sql("show databases").show(false)
    spark.sql("show tables").show(false)
    spark.sql("CREATE TABLE person2 (id int, name string, age int) row format delimited fields terminated by ' '")
    spark.sql("LOAD DATA LOCAL INPATH 'file://data/input/person.txt' INTO TABLE person2")
    spark.sql("show tables").show(false)
    spark.sql("select * from person2").show(false)

    //5.关闭资源
    sc.stop()
    spark.stop()
  }
}
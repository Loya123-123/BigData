package com.yinjz.spark.ODPS

import org.apache.spark.sql.SparkSession

object SparkSQL {
  def main(args: Array[String]) {
    val spark = SparkSession
      .builder()
      .appName("SparkSQL-on-MaxCompute")
      .config("spark.master", "local[4]") // 需要设置spark.master为local[N]才能直接运行，N为并发数。
      .config("spark.hadoop.odps.project.name", "CRM_DataWorks")
      .config("spark.hadoop.odps.access.id", "LTAI4G1JcCucjSQuLGZfqe4A")
      .config("spark.hadoop.odps.access.key", "hp4AhEL6A2r1q7x065sSZbozDT5Ibh")
      .config("spark.hadoop.odps.end.point", "http://service.cn.maxcompute.aliyun.com/api")
      .config("spark.sql.catalogImplementation", "odps")
      .config("spark.sql.broadcastTimeout", 20 * 60)
      .config("spark.sql.crossJoin.enabled", true)
      .config("odps.exec.dynamic.partition.mode", "nonstrict")
      .getOrCreate()

    // val project = spark.conf.get("odps.project.name")

    import spark._
    import sqlContext.implicits._
    val tableName = "mc_test_table"
    val ptTableName = "mc_test_pt_table"
    // Drop Create
    sql(s"DROP TABLE IF EXISTS ${tableName}")
    sql(s"DROP TABLE IF EXISTS ${ptTableName}")

    sql(s"CREATE TABLE ${tableName} (name STRING, num BIGINT)")
    sql(s"CREATE TABLE ${ptTableName} (name STRING, num BIGINT) PARTITIONED BY (pt1 STRING, pt2 STRING)")
    println("建表成功")
    val df = spark.sparkContext.parallelize(0 to 99, 2).map(f => {
      (s"name-$f", f)
    }).toDF("name", "num")

    val ptDf = spark.sparkContext.parallelize(0 to 99, 2).map(f => {
      (s"name-$f", f, "2018", "0601")
    }).toDF("name", "num", "pt1", "pt2")

    // 写 普通表
    df.write.insertInto(tableName) // insertInto语义
    df.write.mode("overwrite").insertInto(tableName) // insertOverwrite语义
    println("普通表写数据成功")
    // 写 分区表
    // DataFrameWriter 无法指定分区写入 需要通过临时表再用SQL写入特定分区
    df.createOrReplaceTempView(s"${ptTableName}_tmp_view")
    sql(s"insert into table ${ptTableName} partition (pt1='2018', pt2='0601') select * from ${ptTableName}_tmp_view")
    sql(s"insert overwrite table ${ptTableName} partition (pt1='2018', pt2='0601') select * from ${ptTableName}_tmp_view")
    println("分区表创建分区")
    ptDf.write.insertInto(ptTableName) // 动态分区 insertInto语义
    ptDf.write.mode("overwrite").insertInto(ptTableName) // 动态分区 insertOverwrite语义
    println("分区表写数据成功")
    // 读 普通表
    val rdf = sql(s"select name, num from $tableName")
    println(s"rdf count, ${rdf.count()}")
    rdf.printSchema()

    // 读 分区表
    val rptdf = sql(s"select name, num, pt1, pt2 from $ptTableName where pt1 = '2018' and pt2 = '0601'")
    println(s"rptdf count, ${rptdf.count()}")
    rptdf.printSchema()
  }
}
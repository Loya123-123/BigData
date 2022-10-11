package com.yinjz.spark.ODPS

import org.apache.spark.sql.SparkSession

import scala.math.random

object SparkPi {
  def main(args: Array[String]) {
    val spark = SparkSession
      .builder()
      .appName("SparkPi")
      .config("spark.master", "local[4]") // 需要设置spark.master为local[N]才能直接运行，N为并发数。
//      .config("spark.hadoop.odps.project.name", "CRM_DataWorks")
//      .config("spark.hadoop.odps.access.id", "LTAI4G1JcCucjSQuLGZfqe4A")
//      .config("spark.hadoop.odps.access.key", "hp4AhEL6A2r1q7x065sSZbozDT5Ibh")
//      .config("spark.hadoop.odps.end.point", "http://service.cn.maxcompute.aliyun.com/api")
//      .config("spark.sql.catalogImplementation", "odps")
      .getOrCreate()
    val sc = spark.sparkContext

    try {
      val slices = if (args.length > 0) args(0).toInt else 2
      val n = math.min(100000L * slices, Int.MaxValue).toInt // avoid overflow
      val count = sc.parallelize(1 until n, slices).map { i =>
        val x = random * 2 - 1
        val y = random * 2 - 1
        if (x * x + y * y < 1) 1 else 0
      }.reduce(_ + _)
      println("Pi is roughly " + 4.0 * count / n)
    } finally {
      sc.stop()
    }
  }
}

package com.yinjz.spark.ODPS

import org.apache.spark.sql.SparkSession

object WordCount {
  def main(args: Array[String]) {
        val spark = SparkSession
          .builder()
          .config("spark.master", "local[4]") // 需要设置spark.master为local[N]才能直接运行，N为并发数。
          .config("spark.hadoop.odps.project.name", "CRM_DataWorks")
          .config("spark.hadoop.odps.access.id", "LTAI4G1JcCucjSQuLGZfqe4A")
          .config("spark.hadoop.odps.access.key", "hp4AhEL6A2r1q7x065sSZbozDT5Ibh")
          .config("spark.hadoop.odps.end.point", "http://service.cn.maxcompute.aliyun.com/api")
          .config("spark.sql.catalogImplementation", "odps")
          .appName("WordCount")
          .getOrCreate()
//    val spark = SparkSession
//      .builder()
//      .appName("WordCount")
//      .getOrCreate()
    val sc = spark.sparkContext

    try {
      sc.parallelize(1 to 100, 10)
        .map(word => (word, 1))
        .reduceByKey(_ + _, 10)
        .take(100).foreach(println)
    } finally {
      sc.stop()
    }
  }
}

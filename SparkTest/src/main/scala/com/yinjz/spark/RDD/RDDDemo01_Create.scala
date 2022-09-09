package com.yinjz.spark.RDD

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Author itcast
 * Desc 演示RDD的多种创建方式
 */
object RDDDemo01_Create {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setAppName("wc").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(sparkConf)
    sc.setLogLevel("WARN")

    //-1 parallelize/makeRDD：基于本地集合
    val rdd1: RDD[String] = sc.parallelize(List("hadoop", "spark", "flink"))
    val rdd2: RDD[String] = sc.parallelize(List("hadoop", "spark", "flink"), 8)
    val rdd3: RDD[String] = sc.makeRDD(List("hadoop", "spark", "flink"), 9) //底层parallelize

    //-2 textFile：基于本地/HDFS文件/文件夹：
    val rdd4: RDD[String] = sc.textFile("data/input/words.txt")
    val rdd5: RDD[String] = sc.textFile("data/input/words.txt", 3)
    //注意:如果使用textFile读取小文件,有多少小文件,就有多少Partition!所以不能用textFile读取小文件
    //RDD[一行行的数据]
    val rdd6: RDD[String] = sc.textFile("data/input/ratings10")
    val rdd7: RDD[String] = sc.textFile("data/input/ratings10", 3)

    //-3 wholeTextFiles：读取小文件
    //注意:应该使用wholeTextFiles读取小文件
    //RDD[(文件名, 文件内容一行行的数据)]
    val rdd8: RDD[(String, String)] = sc.wholeTextFiles("data/input/ratings10")
    val rdd9: RDD[(String, String)] = sc.wholeTextFiles("data/input/ratings10", 3)
    val arr: Array[(String, String)] = rdd9.take(1)
    arr.foreach(println)

    //-4 getNumPartitions/partitions.length：获取分区数
    println("rdd1:" + rdd1.getNumPartitions) //rdd1:8
    println("rdd2:" + rdd2.getNumPartitions) //rdd2:8
    println("rdd3:" + rdd3.getNumPartitions) //rdd3:9

    println("rdd4:" + rdd4.getNumPartitions) //rdd4:2
    println("rdd5:" + rdd5.getNumPartitions) //rdd5:3
    println("rdd6:" + rdd6.getNumPartitions) //rdd6:10
    println("rdd7:" + rdd7.getNumPartitions) //rdd7:10

    println("rdd8:" + rdd8.getNumPartitions) //rdd8:2
    println("rdd9:" + rdd9.getNumPartitions) //rdd9:3
  }
}
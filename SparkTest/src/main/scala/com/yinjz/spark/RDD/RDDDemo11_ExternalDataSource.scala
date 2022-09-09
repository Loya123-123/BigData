package com.yinjz.spark.RDD

import org.apache.commons.lang3.StringUtils
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Author itcast
 * Desc 演示RDD的外部数据源-1
 */
object RDDDemo11_ExternalDataSource{
  def main(args: Array[String]): Unit = {
    //1.准备环境(Env)sc-->SparkContext
    val sparkConf: SparkConf = new SparkConf().setAppName("wc").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(sparkConf)
    sc.setLogLevel("WARN")

    //2.加载数据
    val result: RDD[(String, Int)] = sc.textFile("data/input/words.txt")
      .flatMap(_.split(" "))
      .map((_, 1)) //_表示每一个单词
      .reduceByKey(_ + _)

    //输出为其他格式
    result.coalesce(1).saveAsSequenceFile("data/output/sequence") //保存为序列化文件
    result.coalesce(1).saveAsObjectFile("data/output/object") //保存为对象文件

    //读取其他格式
    val rdd1: RDD[(String, Int)] = sc.sequenceFile("data/output/sequence") //读取sequenceFile
    val rdd2: RDD[(String, Int)] = sc.objectFile("data/output/object") //读取objectFile

    rdd1.foreach(println)
    rdd2.foreach(println)

    sc.stop()
  }
}
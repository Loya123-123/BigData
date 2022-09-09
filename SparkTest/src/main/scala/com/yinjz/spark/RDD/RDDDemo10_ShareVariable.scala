package com.yinjz.spark.RDD


import org.apache.commons.lang3.StringUtils
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Author itcast
 * Desc 演示RDD的广播变量和累加器
 * -1. 过滤标点符号数据
 * 使用广播变量
 * -2. 统计出标点符号数据出现次数
 * 使用累加器
 */
object RDDDemo10_ShareVariable {
  def main(args: Array[String]): Unit = {
    //1.准备环境(Env)sc-->SparkContext
    val sparkConf: SparkConf = new SparkConf().setAppName("wc").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(sparkConf)
    sc.setLogLevel("WARN")

    //2.加载数据
    val dataRDD: RDD[String] = sc.textFile("data/input/words2.txt", minPartitions = 2)

    //3.准备/加载规则(就是一些定义好的特殊字符)
    val ruleList: List[String] = List(",", ".", "!", "#", "$", "%")

    //TODO 将list进行广播,广播到各个Worder(各个Task会去各自的Worker去读)
    val broadcast: Broadcast[List[String]] = sc.broadcast(ruleList)

    //TODO 声明一个累加器
    val accumulator: LongAccumulator = sc.longAccumulator("my-counter")

    //4.统计dataRDD中的WordCount和特殊字符的数量
    val result: RDD[(String, Int)] = dataRDD
      //.filter(line => !line.isEmpty && line.trim().length > 0)
      .filter(StringUtils.isNotBlank(_))
      .flatMap(_.split("\\s+")) //切出单词和特殊字符
      .filter(value => {
        //TODO  获取广播变量的值
        val rules: List[String] = broadcast.value
        if (rules.contains(value)) {
          //TODO 把特殊字符使用累加器进行计总数
          accumulator.add(1)
          false
        } else {
          //把word过滤出进行wordcount
          true
        }
      })
      .map((_, 1))
      .reduceByKey(_ + _)
    println("wordcount的结果为:")
    result.foreach(println)

    println("获取到的累加器的值/特殊字符的总数为:"+accumulator.value)

    sc.stop()
  }
}
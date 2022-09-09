package com.yinjz.spark.RDD

import RDDDemo03_Repartition.MyPartitioner
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Author itcast
 * Desc 演示RDD的聚合函数
 *
 */
object RDDDemo05_Aggregate {
  def main(args: Array[String]): Unit = {
    //1.准备环境(Env)sc-->SparkContext
    val sparkConf: SparkConf = new SparkConf().setAppName("wc").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(sparkConf)
    sc.setLogLevel("WARN")

    //2.加载数据
    val wordAndOne: RDD[(String, Int)] = sc.textFile("data/input/words.txt")
      .flatMap(_.split(" "))
      .map((_, 1))

    //聚合
    //方式1:groupByKey+mapValues+sum/reduce
    val groupedRDD: RDD[(String, Iterable[Int])] = wordAndOne.groupByKey()
    val result1: RDD[(String, Int)] = groupedRDD.mapValues(_.sum)

    //方式2:reduceByKey--开发中使用
    val result2: RDD[(String, Int)] = wordAndOne.reduceByKey(_+_)

    //方式3:foldByKey
    val result3: RDD[(String, Int)] = wordAndOne.foldByKey(0)(_+_)

    //方式4:aggregateByKey
    val result4: RDD[(String, Int)] = wordAndOne.aggregateByKey(0)(_+_,_+_)

    result1.foreach(println)
    result2.foreach(println)
    result3.foreach(println)
    result4.foreach(println)

    //5.关闭资源
    sc.stop()
  }
}
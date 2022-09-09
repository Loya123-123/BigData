package com.yinjz.spark.RDD


import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Author itcast
 * Desc 演示RDD的排序函数
 */
object RDDDemo07_Sort {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setAppName("wc").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(sparkConf)
    sc.setLogLevel("WARN")

    //RDD[(单词, 数量)]
    val result: RDD[(String, Int)] = sc.textFile("data/input/words.txt")
      .flatMap(_.split(" "))
      .map((_, 1))
      .reduceByKey(_ + _)
    result.foreach(println)
    println("============sortBy:适合大数据量排序====================")
    val sortedResult1: RDD[(String, Int)] = result.sortBy(_._2,false)//false表示逆序
    /* println("--------------------------")
     sortedResult1.foreach(println)
     println("--------------------------")*/
    sortedResult1.take(3).foreach(println)

    println("============sortByKey:适合大数据量排序====================")
    //val sortedResult2: RDD[(Int, String)] = result.map(t=>(t._2,t._1)).sortByKey(false)
    val sortedResult2: RDD[(Int, String)] = result.map(_.swap).sortByKey(false)//swap表示交换位置
    sortedResult2.take(3).foreach(println)

    println("============top:适合小数据量排序====================")
    //@note
    // This method should only be used if the resulting array is expected to be small,
    // as all the data is loaded into the driver's memory.
    val sortedResult3: Array[(String, Int)] = result.top(3)(Ordering.by(_._2))//注意:top本身就是取最大的前n个
    sortedResult3.foreach(println)

    sc.stop()
  }
}
package com.yinjz.spark.RDD

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkConf, SparkContext, TaskContext}

/**
 * Author itcast
 * Desc 演示RDD的重分区函数
 *
 */
object RDDDemo03_Repartition {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setAppName("wc").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(sparkConf)
    sc.setLogLevel("WARN")

    val rdd1: RDD[String] = sc.parallelize(List("hadoop", "spark", "flink"), 4)
    println(rdd1.getNumPartitions) //4

    //注意：重分区:改变的是新生成的RDD的分区数,原来的RDD的分区数不变
    //-1.repartition,可以增加和减少分区
    val rdd2: RDD[String] = rdd1.repartition(5)
    println(rdd1.getNumPartitions) //4
    println(rdd2.getNumPartitions) //5

    val rdd3: RDD[String] = rdd1.repartition(3)
    println(rdd1.getNumPartitions) //4
    println(rdd3.getNumPartitions) //3

    //-2.coalesce,默认只能减少分区
    val rdd4: RDD[String] = rdd1.coalesce(5)
    println(rdd1.getNumPartitions) //4
    println(rdd4.getNumPartitions) //4
    val rdd5: RDD[String] = rdd1.coalesce(3)
    println(rdd1.getNumPartitions) //4
    println(rdd5.getNumPartitions) //3


    //-3.默认按照key的hash进行分区
    val resultRDD: RDD[(String, Int)] = sc.textFile("data/input/words.txt")
      .flatMap(_.split(" "))
      .map((_, 1)) //_表示每一个单词
      .reduceByKey(_ + _)
    resultRDD.foreachPartition(iter => {
      iter.foreach(t => {
        val key: String = t._1
        val num: Int = TaskContext.getPartitionId()
        println(s"默认的hash分区器进行的分区:分区编号:${num},key:${key}")
      })
    })

    //-4.自定义分区
    val resultRDD2: RDD[(String, Int)] = sc.textFile("data/input/words.txt")
      .flatMap(_.split(" "))
      .map((_, 1)) //_表示每一个单词
      .reduceByKey(new MyPartitioner(1), _ + _)
    resultRDD2.foreachPartition(iter => {
      iter.foreach(t => {
        val key: String = t._1
        val num: Int = TaskContext.getPartitionId()
        println(s"自定义分区器进行的分区:分区编号:${num},key:${key}")
      })
    })

    sc.stop()
  }

  //自定义的分区器
  class MyPartitioner(partitions: Int) extends Partitioner {
    override def numPartitions: Int = partitions

    override def getPartition(key: Any): Int = {
      0
    }
  }

}
package com.yinjz.spark.RDD

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkConf, SparkContext, TaskContext}

/**
 * Author itcast
 * Desc 演示RDD的聚合函数
 *
 */
object RDDDemo04_Aggregate {
  def main(args: Array[String]): Unit = {
    //1.准备环境(Env)sc-->SparkContext
    val sparkConf: SparkConf = new SparkConf().setAppName("wc").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(sparkConf)
    sc.setLogLevel("WARN")

    //2.加载数据
    val nums: RDD[Int] = sc.parallelize( 1 to 10) //和55

    val result1: Double = nums.sum() //55//底层:fold(0.0)(_ + _)
    val result2: Int = nums.reduce(_+_)//55 注意:reduce是action ,reduceByKey是transformation
    val result3: Int = nums.fold(0)(_+_)//55
    //(zeroValue: U)(seqOp: (U, T) => U, combOp: (U, U) => U)
    //(初始值)(各个分区的聚合函数,多个分区结果的聚合函数)
    val result4: Int = nums.aggregate(0)(_+_,_+_)

    println(result1)
    println(result2)
    println(result3)
    println(result4)

    //5.关闭资源
    sc.stop()
  }
}
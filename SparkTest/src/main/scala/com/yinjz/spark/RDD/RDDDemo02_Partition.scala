package com.yinjz.spark.RDD

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Author itcast
 * Desc 演示RDD的分区操作函数
 *
 */
object RDDDemo02_Partition{
  def main(args: Array[String]): Unit = {
    //1.准备环境(Env)sc-->SparkContext
    val sparkConf: SparkConf = new SparkConf().setAppName("wc").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(sparkConf)
    sc.setLogLevel("WARN")

    //2.加载文件(Source)
    val fileRDD: RDD[String] = sc.textFile("data/input/words.txt")

    //3.处理数据(Transformation)
    val wordRDD: RDD[String] = fileRDD.flatMap(_.split(" "))//_下划线表示每一行
    //map(函数),该函数会作用在每个分区中的每一条数据上,value是每一条数据
    /*val wordAndOneRDD: RDD[(String, Int)] = wordRDD.map(value =>{
      //开启连接-有几条数据就几次
      (value, 1)
      //关闭连接-有几条数据就几次
    })*/

    //mapPartitions(函数):该函数会作用在每个分区上,values是每个分区上的数据
    val wordAndOneRDD: RDD[(String, Int)] = wordRDD.mapPartitions(values => {
      //开启连接-有几个分区就几次
      values.map(value => (value, 1)) //value是该分区中的每一条数据
      //关闭连接-有几个分区就几次
    })

    val resultRDD: RDD[(String, Int)] = wordAndOneRDD.reduceByKey(_+_)

    //4.输出结果(Sink)
    //foreach(函数),该函数会作用在每个分区中的每一条数据上,value是每一条数据
    //Applies a function f to all elements of this RDD.
    /*resultRDD.foreach(value=>{
      //开启连接-有几条数据就几次
      println(value)
      //关闭连接-有几条数据就几次
    })*/

    //foreachPartition(函数),该函数会作用在每个分区上,values是每个分区上的数据
    //Applies a function f to each partition of this RDD.
    resultRDD.foreachPartition(values=>{
      //开启连接-有几个分区就几次
      values.foreach(value=>println(value))//value是该分区中的每一条数据
      //关闭连接-有几个分区就几次
    })

    //5.关闭资源
    sc.stop()
  }
}
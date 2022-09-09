package com.yinjz.sparkstudy

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Author itcast
 * Desc 演示使用Spark开发WordCount-集群版
 */
object WordCount {
  def main(args: Array[String]): Unit = {
    println(this.getClass.getName.stripSuffix("$"))
    if(args.length != 2){
      println("请携带2个参数: input-path and oupput-path")
      System.exit(1)//非0表示非正常退出程序
    }

    //1.准备环境(Env)sc-->SparkContext
    val sparkConf: SparkConf = new SparkConf().setAppName(this.getClass.getName.stripSuffix("$"))//.setMaster("local[*]")
    val sc: SparkContext = new SparkContext(sparkConf)
    sc.setLogLevel("WARN")

    //2.加载文件(Source)
    val fileRDD: RDD[String] = sc.textFile(args(0))//表示启动该程序的时候需要通过参数执行输入数据路径

    //3.处理数据(Transformation)
    //3.1切分
    val wordRDD: RDD[String] = fileRDD.flatMap(_.split(" "))//_下划线表示每一行
    //3.2每个单词记为1
    val wordAndOneRDD: RDD[(String, Int)] = wordRDD.map((_,1))//_表示每一个单词
    //3.3.分组聚合reduceByKey= 先groupByKey + sum或reduce
    val resultRDD: RDD[(String, Int)] = wordAndOneRDD.reduceByKey(_+_)

    //4.输出结果(Sink)
    resultRDD.foreach(println)
    resultRDD.saveAsTextFile(s"${args(1)}_${System.currentTimeMillis()}")

    //Thread.sleep(1000 * 120)//等待2分钟,方便查看webUI:http://localhost:4040/jobs/

    //5.关闭资源
    sc.stop()
  }
}
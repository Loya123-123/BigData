package com.yinjz.spark.streaming

import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 使用SparkStreaming接收Socket数据,node01:9999
 * 实现SparkStreaming程序停止之后再启动时还能够接着上次的结果进行累加
 * 如:
 * 先发送spark,得到spark,1
 * 再发送spark,得到spark,2
 * 再停止程序,然后重新启动
 * 再发送spark,得到spark,3
 */
object SparkStreamingDemo03_StateRecovery {
  val ckpdir = "./ckp"

  def createStreamingContextFunction:StreamingContext={
    //1.创建环境
    val conf: SparkConf = new SparkConf().setAppName(this.getClass.getSimpleName.stripSuffix("$")).setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)
    sc.setLogLevel("WARN")
    val ssc: StreamingContext = new StreamingContext(sc,Seconds(5))
    ssc.checkpoint(ckpdir)

    //2.接收socket数据
    val linesDS: ReceiverInputDStream[String] = ssc.socketTextStream("localhost",9999)

    //3.做WordCount
    val updateFunc= (currentValues:Seq[Int],historyValue:Option[Int])=>{
      //将当前批次的数据和历史数据进行合并作为这一次新的结果!
      val newValue: Int = currentValues.sum + historyValue.getOrElse(0)//getOrElse(默认值)
      Option(newValue)
    }

    val resultDS: DStream[(String, Int)] = linesDS
      .flatMap(_.split(" "))
      .map((_, 1))
      .updateStateByKey(updateFunc)

    //4.输出
    resultDS.print()

    ssc
  }

  def main(args: Array[String]): Unit = {
    val ssc: StreamingContext = StreamingContext.getOrCreate(ckpdir,createStreamingContextFunction _)
    val sc: SparkContext = ssc.sparkContext
    sc.setLogLevel("WARN")

    //5.启动并等待程序停止
    ssc.start()
    ssc.awaitTermination()
    ssc.stop(stopSparkContext = true, stopGracefully = true)
  }
}
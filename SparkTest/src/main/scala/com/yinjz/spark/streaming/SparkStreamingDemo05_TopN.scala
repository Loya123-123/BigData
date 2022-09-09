package com.yinjz.spark.streaming

import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 使用SparkStreaming接收Socket数据,node01:9999
 * 使用窗口计算模拟热搜排行榜:
 * 每隔10s计算最近20s的热搜排行榜!
 */
object SparkStreamingDemo05_TopN {
  def main(args: Array[String]): Unit = {
    //1.创建环境
    val conf: SparkConf = new SparkConf().setAppName(this.getClass.getSimpleName.stripSuffix("$")).setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)
    sc.setLogLevel("WARN")
    val ssc: StreamingContext = new StreamingContext(sc, Seconds(5))
    ssc.checkpoint("./ckp")

    //2.接收socket数据
    val linesDS: ReceiverInputDStream[String] = ssc.socketTextStream("localhost",9999)

    //3.做WordCount
    val wordAndCountDS: DStream[(String, Int)] = linesDS
      .flatMap(_.split(" "))
      .map((_, 1))
      //windowDuration:窗口长度:就算最近多久的数据,必须都是微批间隔的整数倍
      //slideDuration :滑动间隔:就是每隔多久计算一次,,必须都是微批间隔的整数倍
      //每隔10s(slideDuration :滑动间隔)计算最近20s(windowDuration:窗口长度)的热搜排行榜!
      .reduceByKeyAndWindow((v1:Int, v2:Int)=>v1+v2, Seconds(20),Seconds(10))

    //排序取TopN
    //注意:DStream没有直接排序的方法!所以应该调用DStream底层的RDD的排序方法!
    //transform(函数),该函数会作用到DStream底层的RDD上!
    val resultDS: DStream[(String, Int)] = wordAndCountDS.transform(rdd => {
      val sortedRDD: RDD[(String, Int)] = rdd.sortBy(_._2, false)
      val top3: Array[(String, Int)] = sortedRDD.take(3) //取出当前RDD中排好序的前3个热搜词!
      println("======top3--start======")
      top3.foreach(println)
      println("======top3--end======")
      sortedRDD
    })

    //4.输出
    resultDS.print()

    //5.启动并等待程序停止
    ssc.start()
    ssc.awaitTermination()
    ssc.stop(stopSparkContext = true, stopGracefully = true)
  }
}
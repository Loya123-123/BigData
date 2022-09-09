package com.yinjz.spark.streaming

import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 使用SparkStreaming接收Socket数据,node01:9999
 * 窗口长度:要计算多久的数据
 * 滑动间隔:每隔多久计算一次
 * 窗口长度10s > 滑动间隔5s:每隔5s计算最近10s的数据--滑动窗口
 * 窗口长度10s = 滑动间隔10s:每隔10s计算最近10s的数据--滚动窗口
 * 窗口长度10s < 滑动间隔15s:每隔15s计算最近10s的数据--会丢失数据,开发不用
 * 使用窗口计算: 每隔5s(滑动间隔)计算最近10s(窗口长度)的数据!
 */
object SparkStreamingDemo04_Window {
  def main(args: Array[String]): Unit = {
    //1.创建环境
    val conf: SparkConf = new SparkConf().setAppName(this.getClass.getSimpleName.stripSuffix("$")).setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)
    sc.setLogLevel("WARN")
    //batchDuration the time interval at which streaming data will be divided into batches
    //流数据将被划分为批的时间间隔,就是每隔多久对流数据进行一次微批划分!
    val ssc: StreamingContext = new StreamingContext(sc, Seconds(5))

    // The checkpoint directory has not been set. Please set it by StreamingContext.checkpoint()
    //注意:因为涉及到历史数据/历史状态,也就是需要将历史数据/状态和当前数据进行合并,作为新的Value!
    //那么新的Value要作为下一次的历史数据/历史状态,那么应该搞一个地方存起来!
    //所以需要设置一个Checkpoint目录!
    ssc.checkpoint("./ckp")

    //2.接收socket数据
    val linesDS: ReceiverInputDStream[String] = ssc.socketTextStream("localhost",9999)

    //3.做WordCount
    val resultDS: DStream[(String, Int)] = linesDS
      .flatMap(_.split(" "))
      .map((_, 1))
      //windowDuration:窗口长度:就算最近多久的数据,必须都是微批间隔的整数倍
      //slideDuration :滑动间隔:就是每隔多久计算一次,,必须都是微批间隔的整数倍
      //使用窗口计算: 每隔5s(滑动间隔)计算最近10s(窗口长度)的数据!
      .reduceByKeyAndWindow((v1:Int, v2:Int)=>v1+v2, Seconds(10),Seconds(5))

    //总结:实际开发中需要学会的是如何设置windowDuration:窗口长度和slideDuration :滑动间隔
    //如进行如下需求:
    //每隔30分钟(slideDuration :滑动间隔),计算最近24小时(windowDuration:窗口长度)的各个广告点击量,应该进行如下设置:
    //.reduceByKeyAndWindow((v1:Int, v2:Int)=>v1+v2, Minutes(24*60),Minutes(30))
    //每隔10分钟(slideDuration :滑动间隔),更新最近1小时(windowDuration:窗口长度)热搜排行榜
    //.reduceByKeyAndWindow((v1:Int, v2:Int)=>v1+v2, Minutes(60),Minutes(10))

    //4.输出
    resultDS.print()

    //5.启动并等待程序停止
    ssc.start()
    ssc.awaitTermination()
    ssc.stop(stopSparkContext = true, stopGracefully = true)
  }
}
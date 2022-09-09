package com.yinjz.spark.streaming

import org.apache.commons.lang3.StringUtils
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * 基于IDEA集成开发环境，编程实现从TCP Socket实时读取流式数据，对每批次中数据进行词频统计。
 */
object SparkStreamingDemo01_WordCount {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName(this.getClass.getSimpleName.stripSuffix("$")).setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)
    sc.setLogLevel("WARN")
    //batchDuration the time interval at which streaming data will be divided into batches
    //流数据将被划分为批的时间间隔,就是每隔多久对流数据进行一次微批划分!
    val ssc: StreamingContext = new StreamingContext(sc, Seconds(5))

    val inputDStream: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 9999)

    val resultDStream: DStream[(String, Int)] = inputDStream
      .filter(StringUtils.isNotBlank(_))
      .flatMap(_.trim.split("\\s+"))
      .map((_, 1))
      .reduceByKey(_ + _)

    resultDStream.print(10)

    // 启动并等待程序停止
    // 对于流式应用来说，需要启动应用
    ssc.start()
    // 流式应用启动以后，正常情况一直运行（接收数据、处理数据和输出数据），除非人为终止程序或者程序异常停止
    ssc.awaitTermination()
    // 关闭流式应用(参数一：是否关闭SparkContext，参数二：是否优雅的关闭）
    ssc.stop(stopSparkContext = true, stopGracefully = true)
    //注意:
    //上面的代码可以做WordCount,但是只能对当前批次的数据进行累加!
  }
}
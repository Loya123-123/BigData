package com.yinjz.spark.streaming

import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, State, StateSpec, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 使用SparkStreaming接收node1:9999发送的数据并做WordCount-有状态计算
 */
object SparkStreamingDemo02_State {
  def main(args: Array[String]): Unit = {
    //1.准备环境
    val sparkConf: SparkConf = new SparkConf().setAppName("wc").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(sparkConf)
    sc.setLogLevel("WARN")
    //batchDuration the time interval at which streaming data will be divided into batches
    //将流数据划分为微批的时间间隔/微批划分的时间间隔
    val ssc: StreamingContext = new StreamingContext(sc,Seconds(5))

    //The checkpoint directory has not been set. Please set it by StreamingContext.checkpoint()
    //注意:State状态需要设置checkpoint目录
    ssc.checkpoint("./ckp")

    //2.数据
    val socketDStream: ReceiverInputDStream[String] = ssc.socketTextStream("localhost",9999)

    //3.做WordCount
    /*val resultDS: DStream[(String, Int)] = socketDStream
      .flatMap(_.split(" "))
      .map((_, 1))
      .reduceByKey(_ + _)*/

    //========TODO 状态维护:updateStateByKey=========
    //updateStateByKey:每个批次的数据计算都会对所有的key进行state状态更新
    //目标:根据key维护状态,如
    //第一次进来hello haha得到:(hello,1)、(haha,1)
    //第二次进来hello hello  得到(hello,3)、(haha,1)
    //也就是需要将当前进来的数据根据key和历史状态数据进行聚合!
    //currentValues:当前进来的数据,如进来hello hello,其实也就是[1,1]
    //historyValue: 历史状态值,第一次没有历史状态为0,后面其他次就有了
    //返回:currentValues的和+historyValue
    /*val updateFunc = (currentValues:Seq[Int],historyValue:Option[Int])=>{
      if(currentValues.size > 0){
        val newState: Int = currentValues.sum + historyValue.getOrElse(0)
        Some(newState)
      }else{
        historyValue
      }
    }

    val resultDS: DStream[(String, Int)] = socketDStream
      .flatMap(_.split(" "))
      .map((_, 1))
      //updateFunc: (Seq[V], Option[S]) => Option[S]
      .updateStateByKey(updateFunc)*/


    //========TODO 状态维护:mapWithState=========
    //Spark1.6新出的
    //mapWithState:每个批次的数据计算都只会对有数据的进来key进行state状态更新,性能相比updateStateByKey要更高!
    //word:就是key,就是我们输入的单词
    //current:就是当前值
    //state:就是历史值
    //目标:(word,current+state),需要手动更新状态
    val mappingFunc = (word: String, current: Option[Int], state: State[Int]) => {
      val newState = current.getOrElse(0) + state.getOption.getOrElse(0)
      val output = (word, newState)//返回的数据格式
      state.update(newState)//手动更新
      output
    }
    val resultDS: DStream[(String, Int)] = socketDStream
      .flatMap(_.split(" "))
      .map((_, 1))
      //需要传入状态转换规范(其实就是传入一个转换函数)
      .mapWithState(StateSpec.function(mappingFunc))


    //4.输出
    resultDS.print()

    //5.启动并等待结束
    ssc.start()//流程序需要启动
    ssc.awaitTermination()//流程序会一直运行等待数据到来或手动停止
    ssc.stop(true,true)//是否停止sc,是否优雅停机

    //注意:
    //先启动nc -lk 9999
    //再启动程序

  }
}
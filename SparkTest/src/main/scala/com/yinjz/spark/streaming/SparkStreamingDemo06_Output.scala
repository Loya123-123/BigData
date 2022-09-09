package com.yinjz.spark.streaming

import java.sql.{Connection, DriverManager, PreparedStatement, Timestamp}

import org.apache.commons.lang3.time.FastDateFormat
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 使用SparkStreaming接收Socket数据,node01:9999
 * 对上述案例的结果数据输出到控制台外的其他组件,如MySQL/HDFS
 */
object SparkStreamingDemo06_Output {
  def main(args: Array[String]): Unit = {
    //1.创建环境
    val conf: SparkConf = new SparkConf().setAppName(this.getClass.getSimpleName.stripSuffix("$")).setMaster("local[*]")
      //设置数据输出文件系统的算法版本为2
      //https://blog.csdn.net/u013332124/article/details/92001346
      .set("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
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

    resultDS.foreachRDD((rdd,time)=>{
      val df: FastDateFormat = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss")
      val batchTime: String = df.format(time.milliseconds)
      println("-------自定义的输出-------")
      println(s"batchTime:${batchTime}")
      println("-------自定义的输出-------")
      if(!rdd.isEmpty()){
        //-1.输出到控制台
        rdd.foreach(println)
        //-2.输出到HDFS
        rdd.coalesce(1).saveAsTextFile(s"hdfs://node1:8020/wordcount/output-${time.milliseconds}")
        //-3.输出到MySQL
        /*
        CREATE TABLE `t_hotwords` (
        `time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        `word` varchar(255) NOT NULL,
        `count` int(11) DEFAULT NULL,
        PRIMARY KEY (`time`,`word`)
      ) ENGINE=InnoDB DEFAULT CHARSET=utf8;
         */
        rdd.foreachPartition(iter=>{
          val conn: Connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/bigdata?characterEncoding=UTF-8","root","root")
          val sql:String = "REPLACE INTO `t_hotwords` (`time`, `word`, `count`) VALUES (?, ?, ?);"
          val ps: PreparedStatement = conn.prepareStatement(sql)//获取预编译语句对象
          iter.foreach(t=>{
            val word: String = t._1
            val count: Int = t._2
            ps.setTimestamp(1,new Timestamp(time.milliseconds) )
            ps.setString(2,word)
            ps.setInt(3,count)
            ps.addBatch()
          })
          ps.executeBatch()
          ps.close()
          conn.close()
        })
      }
    })

    //5.启动并等待程序停止
    ssc.start()
    ssc.awaitTermination()
    ssc.stop(stopSparkContext = true, stopGracefully = true)
  }
}
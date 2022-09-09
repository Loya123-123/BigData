package com.yinjz.spark.RDD

import com.hankcs.hanlp.HanLP
import org.apache.commons.lang3.StringUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.immutable.StringOps
import scala.collection.mutable

/**
 * Author itcast
 * Desc 搜狗搜索日志分析
 */
object SouGouQueryAnalysis {
  def main(args: Array[String]): Unit = {
    //1.sc
    val sparkConf: SparkConf = new SparkConf().setAppName("wc").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(sparkConf)
    sc.setLogLevel("WARN")

    //2.source
    val logRDD: RDD[String] = sc.textFile("data/input/SogouQ.sample")

    //3.transformation
    //3.0转为样例类封装为一条条的记录对象
    val recordRDD: RDD[SogouRecord] = logRDD.filter(StringUtils.isNotBlank(_)) //过滤出合法数据
      .map(line => {
        val arr: Array[String] = line.split("\\s+")
        SogouRecord(
          arr(0),
          arr(1),
          arr(2),
          arr(3).toInt,
          arr(4).toInt,
          arr(5)
        )
      })
    println("------ 封装数据 ------")
    recordRDD.foreach(println)
    //3.1搜索关键词统计(word,数量),注意:词要做切割
    //flatMap:map+flatten:简单理解为:1个进去多个出来
    val result1: Array[(String, Int)] = recordRDD.flatMap(record => { //年轻人住房问题 出来: [年轻人,住房,问题]
      import scala.collection.JavaConverters._
      val words: StringOps = record.queryWords //年轻人住房问题
      val splitedWords: mutable.Buffer[String] = HanLP.segment(words.replaceAll("\\[|\\]", ""))
        .asScala.map(_.word.trim)
      splitedWords //[年轻人,住房,问题]
    })
      .filter(word => !word.equals(".") && !word.equals("+"))
      .map((_, 1))
      .reduceByKey(_ + _)
      .sortBy(_._2, false)
      .take(10)
    println("搜索关键词统计Top10统计")
    result1.foreach(println)
    /*
    (地震,599)
    (的,476)
    (汶川,430)
    (原因,360)
    (救灾,323)
    (哄抢,321)
    (物资,321)
    (com,278)
    (图片,258)
    (下载,248)
     */

    //3.2用户搜索词汇统计
    val result2 = recordRDD.flatMap(record => { //年轻人住房问题 出来: [(用户id,年轻人),(用户id,住房),(用户id,问题)]
      import scala.collection.JavaConverters._
      val userId: String = record.userId
      val words: StringOps = record.queryWords
      val splitedWords: mutable.Buffer[String] = HanLP.segment(words.replaceAll("\\[|\\]", ""))
        .asScala.map(_.word.trim)
      val tuples: mutable.Buffer[(String, String)] = splitedWords.map(word => {
        (userId, word)
      })
      tuples
    })
      .filter(t => !t._2.equals(".") && !t._2.equals("+"))
      .map((_, 1)) //[((用户id,年轻人),1),((用户id,住房),1)....]
      .reduceByKey(_ + _) //[((用户id,年轻人),数量),((用户id,住房),数量)....]
      .sortBy(_._2, false)
      .take(10)
    println("用户搜索词汇统计Top10统计")
    result2.foreach(println)
    /*
    ((1011517038707826,主题),27)
    ((7230120314300312,全集),21)
    ((7230120314300312,阅读),20)
    ((7650543509505572,治疗),19)
    ((7650543509505572,的),19)
    ((7650543509505572,孤独症),19)
    ((2512392400865138,拳皇),19)
    ((1011517038707826,手机),19)
    ((7650543509505572,儿童),19)
    ((9026201537815861,scat),19)
     */

    //3.3搜索时间段统计(小时:分钟,数量)
    val result3: Array[(String, Int)] = recordRDD.map(record => {
      //00:00:00
      val tiem: String = record.queryTime.substring(0, 5) //[)
      (tiem, 1)
    }).reduceByKey(_ + _)
      .sortBy(_._2, false)
      .take(10)
    println("搜索时间段统计")
    result3.foreach(println)
    /*
    (00:02,1088)
    (00:04,1056)
    (00:03,1051)
    (00:00,1046)
    (00:01,1046)
    (00:06,1036)
    (00:08,1024)
    (00:05,1024)
    (00:07,999)
    (00:09,630)
     */

  }

  /**
   * 用户搜索点击网页记录Record
   *
   * @param queryTime  访问时间，格式为：HH:mm:ss
   * @param userId     用户ID
   * @param queryWords 查询词
   * @param resultRank 该URL在返回结果中的排名
   * @param clickRank  用户点击的顺序号
   * @param clickUrl   用户点击的URL
   */
  case class SogouRecord(
                          queryTime: String,
                          userId: String,
                          queryWords: String,
                          resultRank: Int,
                          clickRank: Int,
                          clickUrl: String
                        )

}

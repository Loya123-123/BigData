package com.yinjz.sparkstudy


import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Author itcast
 * Desc 演示使用Spark开发WordCount-本地版
 */
object WordCount_bak {
  def main(args: Array[String]): Unit = {
    println(this.getClass.getName.stripSuffix("$"))
    //1.准备环境(Env)sc-->SparkContext
    val sparkConf: SparkConf = new SparkConf().setAppName(this.getClass.getName.stripSuffix("$")).setMaster("local[*]")
    val sc: SparkContext = new SparkContext(sparkConf)
    sc.setLogLevel("WARN")

    //2.加载文件(Source)
    //RDD:弹性分布式数据集（RDD），Spark中的基本抽象。
    //先简单理解为分布式集合!类似于DataSet!
    //fileRDD: RDD[一行行的数据]
    val fileRDD: RDD[String] = sc.textFile("data/input/words.txt")

    //3.处理数据(Transformation)
    //3.1切分
    //wordRDD: RDD[一个个的单词]
    //val wordRDD: RDD[String] = fileRDD.flatMap((line)=>line.split(" "))
    val wordRDD: RDD[String] = fileRDD.flatMap(_.split(" "))//_下划线表示每一行
    //3.2每个单词记为1
    // wordAndOneRDD: RDD[(单词, 1)]
    //val wordAndOneRDD: RDD[(String, Int)] = wordRDD.map((word)=>(word,1))
    val wordAndOneRDD: RDD[(String, Int)] = wordRDD.map((_,1))//_表示每一个单词
    //3.3.分组聚合reduceByKey= 先groupByKey + sum或reduce
    //val groupedRDD: RDD[(String, Iterable[Int])] = wordAndOneRDD.groupByKey()
    //val resultRDD: RDD[(String, Int)] = groupedRDD.mapValues(_.sum)
    //val resultRDD: RDD[(String, Int)] = wordAndOneRDD.reduceByKey((temp,current)=>temp + current)
    val resultRDD: RDD[(String, Int)] = wordAndOneRDD.reduceByKey(_+_)

    //4.输出结果(Sink)
    //4.1直接遍历分布式集合并输出
    resultRDD.foreach(println)//cation

    //4.2将分布式集合收集为本地集合再输出
    val array: Array[(String, Int)] = resultRDD.collect()//action
    //println(array.toBuffer)
    //array.foreach(t=>println(t))
    //array.foreach(println(_))
    //array.foreach(println _)
    array.foreach(println)//行为参数化

//    Thread.sleep(1000 * 1200)//等待20分钟,方便查看webUI:http://localhost:4040/jobs/

    //5.关闭资源
    sc.stop()
  }
}
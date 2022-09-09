package com.yinjz.spark.sql

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Author itcast
 * Desc 演示SparkSQL-花式查询-SQL风格和DSL风格
 */
object SparkSQLDemo04_FlowerQuery {
  def main(args: Array[String]): Unit = {
    //1.准备SparkSQL开发环境
    val spark: SparkSession = SparkSession.builder().appName("hello").master("local[*]").getOrCreate()
    val sc: SparkContext = spark.sparkContext
    sc.setLogLevel("WARN")

    //2.获取RDD
    val fileRDD: RDD[String] = sc.textFile("data/input/person.txt")
    val personRDD: RDD[Person] = fileRDD.map(line => {
      val arr: Array[String] = line.split(" ")
      Person(arr(0).toInt, arr(1), arr(2).toInt)
    })
    //3.RDD->DataFrame
    import spark.implicits._ //隐式转换
    val df: DataFrame = personRDD.toDF()

    //4.输出约束和类型
    df.printSchema()
    df.show()

    //TODO =============花式查询============
    println("===========SQL风格========")
    //-1.注册表
    //df.registerTempTable("t_person")
    //df.createOrReplaceGlobalTempView("t_person")//创建一个全局的视图/表,所有SparkSession可用--生命周期太长
    df.createOrReplaceTempView("t_person") //创建一个临时视图/表,该SparkSession可用
    //-2.各种查询
    //=1.查看name字段的数据
    spark.sql("select name from t_person").show(false)
    //=2.查看 name 和age字段数据
    spark.sql("select name,age from t_person").show(false)
    //=3.查询所有的name和age，并将age+1
    spark.sql("select name,age,age+1 from t_person").show(false)
    //=4.过滤age大于等于25的
    spark.sql("select id,name,age from t_person where age >= 25").show(false)
    //=5.统计年龄大于30的人数
    spark.sql("select count(*) from t_person where age > 30").show(false)
    //=6.按年龄进行分组并统计相同年龄的人数
    spark.sql("select age,count(*) from t_person group by age").show(false)
    //=7.查询姓名=张三的
    val name = "zhangsan"
    spark.sql("select id,name,age from t_person where name='zhangsan'").show(false)
    spark.sql(s"select id,name,age from t_person where name='${name}'").show(false)

    println("===========DSL风格========")
    //=1.查看name字段的数据
    df.select(df.col("name")).show(false)
    import org.apache.spark.sql.functions._
    df.select(col("name")).show(false)
    df.select("name").show(false)

    //=2.查看 name 和age字段数据
    df.select("name", "age").show(false)

    //=3.查询所有的name和age，并将age+1
    //df.select("name","age","age+1").show(false)//报错:没有"age+1"这个列名
    //df.select("name","age","age"+1).show(false)//报错:没有"age+1"这个列名
    df.select($"name", $"age", $"age" + 1).show(false) //$"age"表示获取该列的值/$"列名"表示将该列名字符串转为列对象
    df.select('name, 'age, 'age + 1).show(false) //'列名表示将该列名字符串转为列对象

    //=4.过滤age大于等于25的
    df.filter("age >= 25").show(false)
    df.where("age >= 25").show(false)

    //=5.统计年龄大于30的人数
    val count: Long = df.filter("age > 30").count()
    println("年龄大于30的人数"+count)

    //=6.按年龄进行分组并统计相同年龄的人数
    df.groupBy("age").count().show(false)

    //=7.查询姓名=张三的
    df.filter("name ='zhangsan'").show(false)
    df.where("name ='zhangsan'").show(false)
    df.filter($"name" === "zhangsan").show(false)
    df.filter('name === "zhangsan").show(false)
    //=8.查询姓名!=张三的
    df.filter($"name" =!= name).show(false)
    df.filter('name =!= "zhangsan").show(false)


    //TODO =============花式查询============

    //5.关闭资源
    sc.stop()
    spark.stop()
  }

  case class Person(id: Int, name: String, age: Int)

}
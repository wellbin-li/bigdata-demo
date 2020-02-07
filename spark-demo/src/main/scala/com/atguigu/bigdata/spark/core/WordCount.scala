package com.atguigu.bigdata.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * WordCount案例
  */
object WordCount {
  def main(args: Array[String]): Unit = {

    //local模式
    //创建SparkConf对象
    //设定spark计算框架的运行（部署环境）
    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")

    //创建spark上下文对象
    val sc = new SparkContext(config)

    //读取文件，将文件内容一行一行读取出来
    val lines: RDD[String] = sc.textFile("spark-demo/in/*")

    //将一行一行的数据分解成一个一个的单词
    var words: RDD[String] = lines.flatMap(_.split(" "))

    //为了统计方便，将单词数据进行结构的转换
    val wordToOne: RDD[(String, Int)] = words.map((_, 1))

    //对转换结构后的数据进行分组聚合
    val wordToSum: RDD[(String, Int)] = wordToOne.reduceByKey(_+_)

    //将结果采集后打印到控制台
    val result: Array[(String,Int)] = wordToSum.collect()

    result.foreach(println)

    //停止
    sc.stop()

  }
}

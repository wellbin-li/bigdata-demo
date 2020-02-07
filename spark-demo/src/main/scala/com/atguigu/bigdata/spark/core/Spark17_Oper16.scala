package com.atguigu.bigdata.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * reduceByKey()
  *
  * groupByKey对比reduceByKey：
  * 1、均有shuffle
  * 2、reduceByKey在shuffle之前有预聚合操作，性能比groupByKey高
  * 3、若shuffle之前有预聚合操作，性能会提高
  */
object Spark17_Oper16 {
  def main(args: Array[String]): Unit = {

    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")

    //创建spark上下文对象
    val sc = new SparkContext(config)

    val words = Array("one","two","two","three","three","three")

    val worsPairRDD: RDD[(String, Int)] = sc.makeRDD(words).map((_,1))

    // 计算相同 key 对应值的相加结果
    worsPairRDD.reduceByKey(_+_).collect().foreach(println)
  }

}
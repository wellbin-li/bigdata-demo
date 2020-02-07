package com.atguigu.bigdata.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * RDD 中的函数传递和属性传递
  */
object Spark27_Demo {
  def main(args: Array[String]): Unit = {

    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")

    //创建spark上下文对象
    val sc = new SparkContext(config)

    val rdd: RDD[String] = sc.parallelize(Array("hadoop", "spark", "hive", "atguigu"))

    val search = new Search("h")

    //函数传递
    //val match1: RDD[String] = search.getMatch1(rdd)
    //属性传递
    val match1: RDD[String] = search.getMatche2(rdd)
    match1.collect().foreach(println)

    // 释放资源
    sc.stop()

  }

}

class Search(query: String) {
//class Search(query: String) extends Serializable {
  //过滤出包含字符串的数据
  def isMatch(s: String): Boolean = {
    s.contains(query)
  }

  //过滤出包含字符串的 RDD
  def getMatch1(rdd: RDD[String]): RDD[String] = {
    rdd.filter(isMatch)
  }

  //过滤出包含字符串的 RDD
  def getMatche2(rdd: RDD[String]): RDD[String] = {
    val q = query
    rdd.filter(x => x.contains(q))
  }
}
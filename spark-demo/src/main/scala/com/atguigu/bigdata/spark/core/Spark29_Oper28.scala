package com.atguigu.bigdata.spark.core

import org.apache.spark.{SparkConf, SparkContext}

/**
  * RDD 缓存
  */
object Spark29_Oper28 {
  def main(args: Array[String]): Unit = {

    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")

    //创建spark上下文对象
    val sc = new SparkContext(config)

    val rdd = sc.makeRDD(Array("atguigu"))

    // 不缓存
    //val nocache = rdd.map(_.toString+System.currentTimeMillis)
    // 缓存
    val cache = rdd.map(_.toString+System.currentTimeMillis).cache()

    //nocache.collect().foreach(println)
    cache.collect().foreach(println)

    //nocache.collect().foreach(println)
    cache.collect().foreach(println)

    //nocache.collect().foreach(println)
    cache.collect().foreach(println)


  }

}
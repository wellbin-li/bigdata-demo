package com.atguigu.bigdata.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * groupByKey()
  */
object Spark16_Oper15 {
  def main(args: Array[String]): Unit = {

    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")

    //创建spark上下文对象
    val sc = new SparkContext(config)

    val words = Array("one","two","two","three","three","three")

    val wordPairRDD: RDD[(String, Int)] = sc.makeRDD(words).map((_,1))

    val group: RDD[(String, Iterable[Int])] = wordPairRDD.groupByKey()

    group.collect().foreach(println)

    //分组求和
    group.map(t=>(t._1,t._2.sum)).collect().foreach(println)

  }

}
package com.atguigu.bigdata.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * RDD依赖关系
  */
object Spark28_Oper27 {
  def main(args: Array[String]): Unit = {

    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")

    //创建spark上下文对象
    val sc = new SparkContext(config)

    val rdd1 = sc.makeRDD(List("a", "b", "c", "d"))

    val rdd2: RDD[(String, Int)] = rdd1.map((_,1)).reduceByKey(_+_)

    println(rdd2.toDebugString)

    println(rdd2.dependencies)


  }

}
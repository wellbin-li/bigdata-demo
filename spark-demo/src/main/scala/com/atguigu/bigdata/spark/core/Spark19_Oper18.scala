package com.atguigu.bigdata.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * foldByKey()
  * 是一种特殊的aggregateByKey，分区内和分区间操作相同，而aggregateByKey的分区内和分区间操作可以不同
  */
object Spark19_Oper18 {
  def main(args: Array[String]): Unit = {

    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")

    //创建spark上下文对象
    val sc = new SparkContext(config)

    val rdd = sc.parallelize(List((1, 3), (1, 2), (1, 4), (2, 3), (3, 6), (3, 8)), 3)

    rdd.glom().collect().foreach(lists => {
      println(lists.mkString(","))
    })

    val agg: RDD[(Int, Int)] = rdd.foldByKey(0)(_ + _)

    agg.glom().collect().foreach(lists => {
      println(lists.mkString(","))
    })

  }

}
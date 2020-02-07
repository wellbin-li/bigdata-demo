package com.atguigu.bigdata.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * aggregateByKey()
  */
object Spark18_Oper17 {
  def main(args: Array[String]): Unit = {

    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")

    //创建spark上下文对象
    val sc = new SparkContext(config)

    val rdd: RDD[(String, Int)] = sc.parallelize(List(("a", 3), ("a", 2), ("c", 4), ("b", 3), ("c", 6), ("c", 8)), 2)

    rdd.glom().collect().foreach(lists => {
      println(lists.mkString(","))
    })

    val agg: RDD[(String, Int)] = rdd.aggregateByKey(0)(math.max(_, _), _ + _)

    agg.glom().collect().foreach(lists => {
      println(lists.mkString(","))
    })

    val agg1: RDD[(String, Int)] = rdd.aggregateByKey(10)(math.max(_, _), _ + _)

    agg1.glom().collect().foreach(lists => {
      println(lists.mkString(","))
    })

    val agg2: RDD[(String, Int)] = rdd.aggregateByKey(0)(_ + _, _ + _)

    agg2.glom().collect().foreach(lists => {
      println(lists.mkString(","))
    })

  }

}
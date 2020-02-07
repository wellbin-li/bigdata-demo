package com.atguigu.bigdata.spark.core

import org.apache.spark.{SparkConf, SparkContext}

/**
  * combineByKey()
  *
  */
object Spark20_Oper19 {
  def main(args: Array[String]): Unit = {

    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")

    //创建spark上下文对象
    val sc = new SparkContext(config)

    val input = sc.parallelize(Array(("a", 88), ("b", 95), ("a", 91), ("b", 93), ("a", 95), ("b", 98)), 2)

    input.glom().collect().foreach(lists => {
      println(lists.mkString(","))
    })

    val combine = input.combineByKey(
      (_, 1),
      (acc: (Int, Int), v) => (acc._1 + v, acc._2 + 1),
      (acc1: (Int, Int), acc2: (Int, Int))
      => (acc1._1 + acc2._1, acc1._2 + acc2._2))

    combine.glom().collect().foreach(lists => {
      println(lists.mkString(","))
    })

    val result = combine.map { case (key, value) => (key, value._1 / value._2.toDouble) }

    result.glom().collect().foreach(lists => {
      println(lists.mkString(","))
    })

  }

}
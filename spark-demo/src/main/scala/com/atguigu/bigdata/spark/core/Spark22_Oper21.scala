package com.atguigu.bigdata.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * mapValues()
  *
  */
object Spark22_Oper21 {
  def main(args: Array[String]): Unit = {

    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")

    //创建spark上下文对象
    val sc = new SparkContext(config)

    val rdd = sc.parallelize(Array((1, "a"), (1, "d"), (2, "b"), (3, "c")), 2)

    rdd.glom().collect().foreach(lists => {
      println(lists.mkString(","))
    })

    val mapValuesRDD: RDD[(Int, String)] = rdd.mapValues(_ + "|||")

    mapValuesRDD.glom().collect().foreach(lists => {
      println(lists.mkString(","))
    })

  }

}
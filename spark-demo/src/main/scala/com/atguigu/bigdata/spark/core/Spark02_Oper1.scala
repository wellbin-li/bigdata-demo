package com.atguigu.bigdata.spark.core

import org.apache.spark.{SparkConf, SparkContext}

/**
  * map算子
  */
object Spark02_Oper1 {
  def main(args: Array[String]): Unit = {
    //local模式
    //创建SparkConf对象
    //设定spark计算框架的运行（部署环境）
    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")

    //创建spark上下文对象
    val sc = new SparkContext(config)

    // map算子
    val listRDD = sc.makeRDD(1 to 10)

    val mapRDD = listRDD.map(_ * 2)

    mapRDD.collect().foreach(println)

  }

}

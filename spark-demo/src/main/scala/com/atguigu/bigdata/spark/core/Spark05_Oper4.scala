package com.atguigu.bigdata.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * flatMap算子
  */
object Spark05_Oper4 {
  def main(args: Array[String]): Unit = {

    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")

    //创建spark上下文对象
    val sc = new SparkContext(config)

    val listRDD: RDD[List[Int]] = sc.makeRDD(Array(List(1, 2), List(3, 4)))

    //flatMap
    //1,2,3,4
    val flatMapRDD: RDD[Int] = listRDD.flatMap(datas=>datas)

    flatMapRDD.collect().foreach(println)


  }

}

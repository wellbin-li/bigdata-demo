package com.atguigu.bigdata.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * sortBy()
  */
object Spark13_Oper12 {
  def main(args: Array[String]): Unit = {

    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")

    //创建spark上下文对象
    val sc = new SparkContext(config)

    val listRDD: RDD[Int] = sc.makeRDD(List(1, 3, 5, 1, 2, 6, 2))

    //按照自身大小降序排序
    //val sortByRDD: RDD[Int] = listRDD.sortBy(x=>x,false)
    //按照与 3 余数的大小排序
    val sortByRDD: RDD[Int] = listRDD.sortBy(x=>x%3)

    sortByRDD.collect().foreach(println)

  }

}

package com.atguigu.bigdata.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * sortByKey()
  *
  */
object Spark21_Oper20 {
  def main(args: Array[String]): Unit = {

    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")

    //创建spark上下文对象
    val sc = new SparkContext(config)

    val rdd = sc.parallelize(Array((3,"aa"),(6,"cc"),(2,"bb"),(1,"dd")),1)

    rdd.glom().collect().foreach(lists => {
      println(lists.mkString(","))
    })

   //val sortByKeyRDD: RDD[(Int, String)] = rdd.sortByKey(true)
    val sortByKeyRDD: RDD[(Int, String)] = rdd.sortByKey(false)

    sortByKeyRDD.glom().collect().foreach(lists => {
      println(lists.mkString(","))
    })

  }

}
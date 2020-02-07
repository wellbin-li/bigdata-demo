package com.atguigu.bigdata.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * coalesce和repartition
  */
object Spark12_Oper11 {
  def main(args: Array[String]): Unit = {

    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")

    //创建spark上下文对象
    val sc = new SparkContext(config)

    val listRDD: RDD[Int] = sc.makeRDD(List(1, 3, 5, 1, 2, 6, 2))

    //listRDD.coalesce(2)
    //repartition底层调用coalesce，不过shuffle设置为true，而coalesce可以自由设置shuffle是否为true
    listRDD.repartition(2)

  }

}

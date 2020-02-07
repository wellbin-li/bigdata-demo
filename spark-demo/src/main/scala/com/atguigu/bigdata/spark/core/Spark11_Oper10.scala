package com.atguigu.bigdata.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * coalesce算子
  */
object Spark11_Oper10 {
  def main(args: Array[String]): Unit = {

    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")

    //创建spark上下文对象
    val sc = new SparkContext(config)

    val listRDD: RDD[Int] = sc.makeRDD(1 to 16, 4)

    println("缩减分区前：" + listRDD.partitions.size)

    //缩减分区数，可以简单理解为合并分区
    val coalesceRDD: RDD[Int] = listRDD.coalesce(3)

    println("缩减分区后：" + coalesceRDD.partitions.size)

    coalesceRDD.saveAsTextFile("output")

  }

}

package com.atguigu.bigdata.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * distinct算子
  * 会有shuffle阶段，性能慢
  * shuffle阶段：一个分区的数据打乱重组到不同分区
  */
object Spark10_Oper9 {
  def main(args: Array[String]): Unit = {

    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")

    //创建spark上下文对象
    val sc = new SparkContext(config)

    val listRDD: RDD[Int] = sc.makeRDD(List(1,2,3,4,5,4,3,2,1))

    //val distinctRDD: RDD[Int] = listRDD.distinct()
    // 使用distinct算子对数据去重，但是去重后数据减少，所以可以改变分区的数量
    val distinctRDD: RDD[Int] = listRDD.distinct(2)

    //distinctRDD.collect().foreach(println)
    distinctRDD.saveAsTextFile("output")
  }

}

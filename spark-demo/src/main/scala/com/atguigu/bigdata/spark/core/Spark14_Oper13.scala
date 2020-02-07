package com.atguigu.bigdata.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 双 Value 类型
  * union()、subtract()、intersection()、cartesian()、zip()
  */
object Spark14_Oper13 {
  def main(args: Array[String]): Unit = {

    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")

    //创建spark上下文对象
    val sc = new SparkContext(config)

    val rdd1: RDD[Int] = sc.makeRDD(1 to 6)

    val rdd2: RDD[Int] = sc.makeRDD(4 to 10)

    val rdd3: RDD[Int] = sc.makeRDD(Array(1, 2, 3), 3)

    val rdd4: RDD[String] = sc.makeRDD(Array("a", "b", "c"), 3)

    // union
    val unionRDD: RDD[Int] = rdd1.union(rdd2)
    // subtract
    val substractRDD: RDD[Int] = rdd1.subtract(rdd2)
    // intersection
    val intersectionRDD: RDD[Int] = rdd1.intersection(rdd2)
    // cartesian
    val cartesianRDD: RDD[(Int, Int)] = rdd1.cartesian(rdd2)
    // zip
    val zipRDD: RDD[(Int, String)] = rdd3.zip(rdd4)

    //unionRDD.collect().foreach(println)
    //substractRDD.collect().foreach(println)
    //intersectionRDD.collect().foreach(println)
    //cartesianRDD.collect().foreach(println)
    zipRDD.collect().foreach(println)

  }

}

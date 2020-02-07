package com.atguigu

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object TestScala {

  def main(args: Array[String]): Unit = {


    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("TestScala")

    val sc = new SparkContext(sparkConf)

    //
    val rdd: RDD[String] = sc.textFile("")

    //
    val word: RDD[String] = rdd.flatMap(_.split("\t"))

    //
    val wordAndOne: RDD[(String, Int)] = word.map((_, 1))
    val wordAndCount: RDD[(String, Int)] = wordAndOne.reduceByKey(_ + _)
    wordAndCount.saveAsTextFile("")

    sc.textFile("").flatMap(_.split("\t")).map((_, 1)).reduceByKey(_ + _).saveAsTextFile("")

    sc.stop()

  }


}

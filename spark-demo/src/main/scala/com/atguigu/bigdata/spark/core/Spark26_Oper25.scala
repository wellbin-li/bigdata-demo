package com.atguigu.bigdata.spark.core

import org.apache.spark.{SparkConf, SparkContext}

/**
 * saveAsTextFile() saveAsSequenceFile() saveAsObjectFile
 */
object Spark26_Oper25 {
  def main(args: Array[String]): Unit = {

    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")

    //创建spark上下文对象
    val sc = new SparkContext(config)

    val rdd = sc.makeRDD(Array(("a", 1),("b", 2), ("c", 3)))

    rdd.saveAsTextFile("output1")

    rdd.saveAsSequenceFile("output2")

    rdd.saveAsObjectFile("output3")

  }

}
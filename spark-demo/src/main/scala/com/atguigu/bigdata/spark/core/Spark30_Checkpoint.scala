package com.atguigu.bigdata.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * RDD CheckPoint
  */
object Spark30_Checkpoint {
  def main(args: Array[String]): Unit = {

    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")

    //创建spark上下文对象
    val sc = new SparkContext(config)

    //设定检查点的保存目录
    sc.setCheckpointDir("checkpointdir")

    val rdd = sc.makeRDD(List(1,2,3,4))

    val mapRDD: RDD[(Int, Int)] = rdd.map((_,1))

    //mapRDD.checkpoint()

    val reduceByKeyRDD: RDD[(Int, Int)] = mapRDD.reduceByKey(_+_)

    reduceByKeyRDD.checkpoint()

    reduceByKeyRDD.foreach(println)

    println(reduceByKeyRDD.toDebugString)

  }

}
package com.atguigu.bigdata.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * join()、cogroup()
  *
  */
object Spark23_Oper22 {
  def main(args: Array[String]): Unit = {

    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")

    //创建spark上下文对象
    val sc = new SparkContext(config)

    val rdd = sc.parallelize(Array((1,"a"),(2,"b"),(3,"c")))

    val rdd1 = sc.parallelize(Array((1,4),(2,5),(3,6),(4,7)))

    val joinRDD: RDD[(Int, (String, Int))] = rdd.join(rdd1)

    joinRDD.glom().collect().foreach(lists => {
      println(lists.mkString(","))
    })

    val rdd3 = sc.parallelize(Array((1,"aaa"),(2,"bbb"),(3,"ccc")))

    val joinRDD2: RDD[(Int, ((String, Int), String))] = joinRDD.join(rdd3)

    joinRDD2.glom().collect().foreach(lists => {
      println(lists.mkString(","))
    })

    val cogroupRDD: RDD[(Int, (Iterable[String], Iterable[Int]))] = rdd.cogroup(rdd1)

    cogroupRDD.glom().collect().foreach(lists => {
      println(lists.mkString(","))
    })

  }

}
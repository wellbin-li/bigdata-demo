package com.atguigu.bigdata.spark.core

import org.apache.spark.{SparkConf, SparkContext}

/**
 * reduce()、collect()、count()、first()、take()、takeOrdered()、aggregate()、fold()、countByKey()、foreach()
 */
object Spark25_Oper24 {
  def main(args: Array[String]): Unit = {

    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")

    //创建spark上下文对象
    val sc = new SparkContext(config)

    // reduce
    val rdd1 = sc.makeRDD(1 to 10, 2)
    val rdd2: Int = rdd1.reduce(_ + _)
    println(rdd2)
    val rdd3 = sc.makeRDD(Array(("a", 1),("a", 3), ("e", 3), ("c", 3), ("d", 5)))
    val rdd4: (String, Int) = rdd3.reduce((x, y) => (x._1 + y._1, x._2 + y._2))
    println(rdd4)

    // collect
    rdd1.collect().foreach(println)

    // count
    println(rdd1.count())

    // first
    println(rdd1.first())

    // take
    rdd3.take(3).foreach(print)

    // takeOrdered
    rdd3.takeOrdered(2).foreach(print)

    // aggregate
    println(rdd1.aggregate(0)(_ + _, _ + _))

    // fold
    println(rdd1.fold(0)(_ + _))

    // countByKey
    rdd3.countByKey().foreach(print)

    // foreach
    rdd1.foreach(print(_))

  }

}
package com.atguigu.bigdata.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * filter算子
  */
object Spark08_Oper7 {
  def main(args: Array[String]): Unit = {

    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")

    //创建spark上下文对象
    val sc = new SparkContext(config)

    //生成数据：按照指定的规则进行数据过滤
    val listRDD: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))

    val filterRDD: RDD[Int] = listRDD.filter(x=>x%2==0)

    filterRDD.collect().foreach(println)
  }

}

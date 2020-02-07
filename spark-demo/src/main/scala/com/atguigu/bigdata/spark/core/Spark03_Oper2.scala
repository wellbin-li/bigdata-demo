package com.atguigu.bigdata.spark.core

import org.apache.spark.{SparkConf, SparkContext}

/**
  * mapPartitions算子
  */
object Spark03_Oper2 {
  def main(args: Array[String]): Unit = {
    //local模式
    //创建SparkConf对象
    //设定spark计算框架的运行（部署环境）
    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")

    //创建spark上下文对象
    val sc = new SparkContext(config)

    // mapPartitions
    val listRDD = sc.makeRDD(1 to 10)

    //mapPartitions可以对一个RDD中所有的分区进行遍历
    //mapPartitions效率优于map算子，减少了发送到执行器执行交互次数
    //mapPartitions可能会出现内存溢出OOM
    val mapPartitionsRDD = listRDD.mapPartitions(datas => {
      datas.map(data => data * 2)
    })

    mapPartitionsRDD.collect().foreach(println)

  }

}

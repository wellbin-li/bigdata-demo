package com.atguigu.bigdata.spark.core

import org.apache.spark.{SparkConf, SparkContext}

/**
  * mapPartitionsWithIndex算子
  */
object Spark04_Oper3 {
  def main(args: Array[String]): Unit = {
    //local模式
    //创建SparkConf对象
    //设定spark计算框架的运行（部署环境）
    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")

    //创建spark上下文对象
    val sc = new SparkContext(config)

    // mapPartitions
    val listRDD = sc.makeRDD(1 to 10, 2)

    val tupleRDD = listRDD.mapPartitionsWithIndex {
      case (num, datas) => {
        datas.map((_, "分区号：", num))
      }
    }

    tupleRDD.collect().foreach(println)

  }

}

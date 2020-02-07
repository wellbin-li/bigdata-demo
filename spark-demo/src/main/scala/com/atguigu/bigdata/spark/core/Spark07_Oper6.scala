package com.atguigu.bigdata.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * groupBy算子
  */
object Spark07_Oper6 {
  def main(args: Array[String]): Unit = {

    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")

    //创建spark上下文对象
    val sc = new SparkContext(config)

    //生成数据：按照指定的规则进行分组
    //分组后的数据形成了对偶元组（k-v）,k表示分组的key,v表示分组的数据集合
    val listRDD: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))

    val groupByRDD: RDD[(Int, Iterable[Int])] = listRDD.groupBy(i=>i%2)

    groupByRDD.collect().foreach(println)
  }

}

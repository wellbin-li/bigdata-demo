package com.atguigu.bigdata.spark.core

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 读取数据3种方式
  */
object Spark01_RDD {
  def main(args: Array[String]): Unit = {
    //local模式
    //创建SparkConf对象
    //设定spark计算框架的运行（部署环境）
    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")

    //创建spark上下文对象
    val sc = new SparkContext(config)

    //创建RDD
    //1)从内存中创建makeRDD，底层实现就是
    //val listRDD = sc.makeRDD(List(1,2,3,4))

    //使用自定义分区数
    //val listRDD = sc.makeRDD(List(1,2,3,4),8)

    //val value = sc.makeRDD(Array("1","2","3"))

    //2)从内存中创建parallelize
    //val arrayRDD = sc.parallelize(Array(1,2,3,4))

    //3)从外部存储中创建
    //默认情况下，可以读取项目路径，也可以读取其他路径，HDFS
    //默认从文件中读取的数据都是字符串类类型
    //读取文件时，传递的分区参数为最小分区数，但是不一定是这个分区数，取决于hadoop读取文件时分区规则
    val fileRDD = sc.textFile("in", 2)

    //listRDD.collect().foreach(println)

    //将RDD的数据保存到文件中
    fileRDD.saveAsTextFile("output")

  }

}

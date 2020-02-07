package com.atguigu.bigdata.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * sparksql初识
  */
object SparkSQL01_Demo {
  def main(args: Array[String]): Unit = {

    // SparkConf
    // 1.创建配置对象
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSQL01_Demo")

    // SparkSession
    // 2.创建sparksql环对象
    // val spark = new SparkSession(sparkConf) // 私有构造方法 无法访问
    val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

    // 3.读取数据 构建DataFrame
    val frame: DataFrame = spark.read.json("in/user.json")

    // 4.展示数据
    frame.show()

    // 释放资源
    spark.stop()


  }

}

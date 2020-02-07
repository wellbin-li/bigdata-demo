package com.atguigu.bigdata.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * sql访问
  */
object SparkSQL02_SQL {
  def main(args: Array[String]): Unit = {

    // SparkConf
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSQL01_Demo")

    // SparkSession
    val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

    val frame: DataFrame = spark.read.json("in/user.json")

    // 将DataFrame装换为一张表
    frame.createOrReplaceTempView("user")

    // 采用sql的语法访问数据
    spark.sql("select * from user").show()

    // 释放资源
    spark.stop()


  }

}

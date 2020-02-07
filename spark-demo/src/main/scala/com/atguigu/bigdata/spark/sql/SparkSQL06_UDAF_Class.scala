package com.atguigu.bigdata.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Aggregator

/**
  * UDAF 聚合函数(强类型)
  */
object SparkSQL06_UDAF_Class {
  def main(args: Array[String]): Unit = {

    // SparkConf
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSQL01_Demo")

    // SparkSession
    val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

    // 进行转换之前，需要引入隐式转换规则
    // 这里的spark不是包名的含义，而是SparkSession对象的名称
    import spark.implicits._

    // 自定义聚合函数
    // 创建聚合函数对象
    var udaf = new MyAgeAvgFunction1

    // 将聚合函数转换为查询列
    val avgCol: TypedColumn[UserBean, Double] = udaf.toColumn.name("avgAge")

    val frame: DataFrame = spark.read.json("in/user.json")

    val userDS: Dataset[UserBean] = frame.as[UserBean]

    // 应用函数
    userDS.select(avgCol).show()

    // 释放资源
    spark.stop()


  }

}

case class UserBean(name: String, age: BigInt)

case class AvgBuffer(var sum: BigInt, var count: Int)

// 声明用户自定义聚合函数(强类型)
// 1.继承Aggregator，设定泛型
// 2.实现方法
class MyAgeAvgFunction1 extends Aggregator[UserBean, AvgBuffer, Double] {

  // 初始化
  override def zero: AvgBuffer = {
    AvgBuffer(0, 0)
  }

  /**
    * 聚合数据
    *
    * @param b
    * @param a
    * @return
    */
  override def reduce(b: AvgBuffer, a: UserBean): AvgBuffer = {
    b.sum = b.sum + a.age
    b.count = b.count + 1
    b
  }

  // 缓冲区的合并操作
  override def merge(b1: AvgBuffer, b2: AvgBuffer): AvgBuffer = {
    b1.sum = b1.sum + b2.sum
    b1.count = b1.count + b2.count
    b1
  }

  // 完成计算
  override def finish(reduction: AvgBuffer): Double = {
    reduction.sum.toDouble / reduction.count
  }

  override def bufferEncoder: Encoder[AvgBuffer] = Encoders.product

  override def outputEncoder: Encoder[Double] = Encoders.scalaDouble
}

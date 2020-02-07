package com.atguigu.bigdata.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 累加器:分布式只写共享变量
 */
object Spark31_ShareData {
  def main(args: Array[String]): Unit = {

    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")

    //创建spark上下文对象
    val sc = new SparkContext(config)

    val dataRDD: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 2)

    //val i: Int = dataRDD.reduce(_+_)
    //println(i)

    var sum = 0

    //使用累加器共享变量，来累加数据

    //创建累加器对象
    val accumulator: LongAccumulator = sc.longAccumulator

    dataRDD.foreach{
      case i => {
        //执行累加器的累加功能
        accumulator.add(i)
      }
    }
    //获取累加器的值
    println("sum=" + accumulator.value)

    sc.stop()
  }

}
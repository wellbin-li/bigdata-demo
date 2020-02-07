package com.atguigu.bigdata.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, Partitioner, SparkConf, SparkContext}

/**
  * partitionBy()
  */
object Spark15_Oper14 {
  def main(args: Array[String]): Unit = {

    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")

    //创建spark上下文对象
    val sc = new SparkContext(config)

    val listRDD: RDD[(Int, String)] = sc.makeRDD(Array((1, "aaa"), (2, "bbb"), (3, "ccc"), (4, "ddd")))

    println(listRDD.partitions.size)
    listRDD.glom().collect().foreach(arrays => {
      println(arrays.mkString(","))
    })

    // 使用分区器
    val partitionByRDD: RDD[(Int, String)] = listRDD.partitionBy(new HashPartitioner(2))

    println(partitionByRDD.partitions.size)

    partitionByRDD.glom().collect().foreach(arrays => {
      println(arrays.mkString(","))
    })

    // 使用自定义分区器
    val partRDD: RDD[(Int, String)] = listRDD.partitionBy(new MyPartitions(3))

    println(partRDD.partitions.size)

    partRDD.glom().collect().foreach(arrays => {
      println(arrays.mkString(","))
    })

  }

}

// 声明自定义分区器
// 继承Partitioner类
class MyPartitions(partitions: Int) extends Partitioner {
  override def numPartitions: Int = {
    partitions
  }

  override def getPartition(key: Any): Int = {
    1
  }
}

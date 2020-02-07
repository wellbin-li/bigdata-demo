package com.atguigu.bigdata.spark.core

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 广播变量:分布式只读共享变量
 */
object Spark33_Var {
  def main(args: Array[String]): Unit = {

    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")

    //创建spark上下文对象
    val sc = new SparkContext(config)

    val rdd1: RDD[(Int, String)] = sc.makeRDD(List((1,"a"),(2,"b"),(3,"c")))
    //val rdd2: RDD[(Int, Int)] = sc.makeRDD(List((1,1),(2,2),(3,3)))
    //val joinRDD: RDD[(Int, (String, Int))] = rdd1.join(rdd2)
    //joinRDD.foreach(println)
    /*
    (3,(c,3))
    (2,(b,2))
    (1,(a,1))
     */

    val list = List((1,1),(2,2),(3,3))

    // 可以使用广播变量减少数据的传输
    // 1.构建广播变量
    val broadcast: Broadcast[List[(Int, Int)]] = sc.broadcast(list)

    val resultRDD: RDD[(Int, (String, Any))] = rdd1.map {
      case (key, value) => {
        var v2: Any = null
        // 2.使用广播变量
        for (t <- broadcast.value) {
          if (key == t._1) {
            v2 = t._2
          }
        }
        (key, (value, v2))
      }
    }
    resultRDD.foreach(println)


    sc.stop()
  }

}
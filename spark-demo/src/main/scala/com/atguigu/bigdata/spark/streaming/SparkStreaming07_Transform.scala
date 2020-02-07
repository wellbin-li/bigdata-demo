package com.atguigu.bigdata.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 转换
  *
  */
object SparkStreaming07_Transform {

  def main(args: Array[String]): Unit = {

    // Spark配置对象
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming01_WordCount")

    // 实时数据分析环境对象
    // 采集周期：以指定的时间为周期采集实时数据
    val streamingContext = new StreamingContext(sparkConf, Seconds(3))

    // 从指定的端口中采集数据
    val socketLineDStream: ReceiverInputDStream[String] = streamingContext.socketTextStream("hadoop100", 1008)

    // 转换
    // TODO 代码(Driver) (1)
    // val a = 1
//    socketLineDStream.map {
//      case x => {
//        // TODO 代码(Executor) (n)
//        // val a = 1
//        x
//      }
//    }

    // TODO 代码(Driver) (1)
//    socketLineDStream.transform {
//      case rdd => {
//        // TODO 代码(Driver) (m=采集周期)
//        rdd.map {
//          case x => {
//            // TODO 代码(Driver) (1)
//            x
//          }
//        }
//      }
//    }

//    socketLineDStream.foreachRDD(rdd => {
//      rdd.foreach(println)
//    })

    // 启动采集器
    streamingContext.start()
    // Driver等待采集器的执行
    streamingContext.awaitTermination()
  }

}

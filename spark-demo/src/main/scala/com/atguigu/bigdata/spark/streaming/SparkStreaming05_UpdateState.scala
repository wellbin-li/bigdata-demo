package com.atguigu.bigdata.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 有状态数据统计
  *
  */
object SparkStreaming05_UpdateState {

  def main(args: Array[String]): Unit = {

    // Spark配置对象
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming05_UpdateState")

    // 实时数据分析环境对象
    // 采集周期：以指定的时间为周期采集实时数据
    val streamingContext = new StreamingContext(sparkConf, Seconds(3))

    // 保存数据的状态，需要设定检查点路径
    streamingContext.sparkContext.setCheckpointDir("cp")

    // 从kafka中采集数据
    val kafkaDStream: ReceiverInputDStream[(String, String)] = KafkaUtils.createStream(streamingContext, "hadoop100:2181,hadoop101:2181,hadoop102:2181/kafka", "auguigu", Map("auguigu" -> 3))

    // 将采集的数据进行分解（扁平化）
    val wordDStream: DStream[String] = kafkaDStream.flatMap(t => t._2.split(" "))

    // 将数据进行结构的转换方便统计分析
    val mapDStream: DStream[(String, Int)] = wordDStream.map((_, 1))

    // 将转换结构后的数据进行聚合处理
    //val wordToSumDStream: DStream[(String, Int)] = mapDStream.reduceByKey(_ + _)
    val stateDstream: DStream[(String, Int)] = mapDStream.updateStateByKey {
      case (seq, buffer) => {
        val sum = buffer.getOrElse(0) + seq.sum
        Option(sum)
      }
    }

    // 打印结果
    // wordToSumDStream.print()
    stateDstream.print()

    // 不能停止采集程序
    // streamingContext.stop()

    // 启动采集器
    streamingContext.start()
    // Driver等待采集器的执行
    streamingContext.awaitTermination()
  }

}

package com.atguigu.bigdata.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 从kafka中采集数据
  *
  * 创建topic命令：
  * bin/kafka-topics.sh \
  * --create \
  * --zookeeper hadoop100:2181,hadoop101:2181,hadoop102:2181/kafka \
  * --replication-factor 2 \
  * --partitions 3 \
  * --topic auguigu
  *
  * 查询topic命令：
  * bin/kafka-topics.sh --zookeeper hadoop100:2181,hadoop101:2181,hadoop102:2181/kafka --list
  *
  * 生产数据命令：
  * bin/kafka-console-producer.sh --broker-list hadoop100:9092,hadoop101:9092,hadoop102:9092 --topic auguigu
  *
  * 需要添加中的org/apache/spark/Logging类
  * <dependency>
  * <groupId>org.apache.spark</groupId>
  * <artifactId>spark-core_2.10</artifactId>
  * <version>1.5.2</version>
  * <scope>compile</scope>
  * </dependency>
  *
  */
object SparkStreaming04_KafkaSource {

  def main(args: Array[String]): Unit = {

    // Spark配置对象
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming04_KafkaSource")

    // 实时数据分析环境对象
    // 采集周期：以指定的时间为周期采集实时数据
    val streamingContext = new StreamingContext(sparkConf, Seconds(3))

    // 从kafka中采集数据
    val kafkaDStream: ReceiverInputDStream[(String, String)] = KafkaUtils.createStream(streamingContext, "hadoop100:2181,hadoop101:2181,hadoop102:2181/kafka", "auguigu", Map("auguigu" -> 3))

    // 将采集的数据进行分解（扁平化）
    val wordDStream: DStream[String] = kafkaDStream.flatMap(t=>t._2.split(" "))

    // 将数据进行结构的转换方便统计分析
    val mapDStream: DStream[(String, Int)] = wordDStream.map((_, 1))

    // 将转换结构后的数据进行聚合处理
    val wordToSumDStream: DStream[(String, Int)] = mapDStream.reduceByKey(_ + _)

    // 打印结果
    wordToSumDStream.print()

    // 不能停止采集程序
    // streamingContext.stop()

    // 启动采集器
    streamingContext.start()
    // Driver等待采集器的执行
    streamingContext.awaitTermination()
  }

}

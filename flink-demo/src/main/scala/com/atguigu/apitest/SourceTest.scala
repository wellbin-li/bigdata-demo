package com.atguigu.apitest

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011

// 温度传感器读取样例类
case class SensorReading(id: String, timestamp: Long, temparature: Double)

object SourceTest {

  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    /*
    // 1.从自定义的集合中读取数据
    val stream1: DataStream[SensorReading] = env.fromCollection(List(
      SensorReading("sensor_1", 1547718199, 35.80),
      SensorReading("sensor_6", 1547718201, 15.40),
      SensorReading("sensor_7", 1547718202, 6.72),
      SensorReading("sensor_10", 1547718205, 38.10)
    ))

    stream1.print("stream1").setParallelism(1)
    */

    //env.fromElements(1, 2.0, "string").print()

    /*
    // 2.从文件中读取数据
    val stream2: DataStream[String] = env.readTextFile("flink-demo/in/sensor.txt")

    stream2.print("stream2").setParallelism(1)
    */

    // 3.从kafka中读取数据
    // 创建topic
    // /usr/local/kafka/bin/kafka-topics.sh --zookeeper hadoop100:2181,hadoop101:2181,hadoop102:2181 --create --replication-factor 3 --partitions 3 --topic sensor
    // 发送消息
    // /usr/local/kafka/bin/kafka-console-producer.sh --broker-list hadoop100:9092,hadoop101:9092,hadoop102:9092 --topic sensor
    // 消费消息
    // /usr/local/kafka/bin/kafka-console-consumer.sh --bootstrap-server hadoop100:9092,hadoop101:9092,hadoop102:9092 --from-beginning --topic sensor
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "hadoop100:9092,hadoop101:9092,hadoop102:9092")
    properties.setProperty("group.id", "test-consumer-group")
    properties.setProperty("key.desrializer", "org.apache.kafka.common.serialization.StringDesrializer")
    properties.setProperty("value.desrializer", "org.apache.kafka.common.serialization.StringDesrializer")
    properties.setProperty("auto.offset.reset", "latest")

    val stream3: DataStream[String] = env.addSource(new FlinkKafkaConsumer011[String]("sensor", new SimpleStringSchema(), properties))

    stream3.print("stream3").setParallelism(1)

    env.execute("source test")

  }

}

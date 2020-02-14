package com.atguigu.apitest.sinktest

import java.util.Properties
import com.atguigu.apitest.SensorReading
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer011, FlinkKafkaProducer011}

object KafkaSinkTest {

  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    // Source
    // 发送消息：/usr/local/kafka/bin/kafka-console-producer.sh --broker-list hadoop100:9092,hadoop101:9092,hadoop102:9092 --topic sensor
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "hadoop100:9092,hadoop101:9092,hadoop102:9092")
    properties.setProperty("group.id", "test-consumer-group")
    properties.setProperty("key.desrializer", "org.apache.kafka.common.serialization.StringDesrializer")
    properties.setProperty("value.desrializer", "org.apache.kafka.common.serialization.StringDesrializer")
    properties.setProperty("auto.offset.reset", "latest")

    val inputStream: DataStream[String] = env.addSource(new FlinkKafkaConsumer011[String]("sensor", new SimpleStringSchema(), properties))


    // Transform
    val dataStream: DataStream[String] = inputStream.map(data => {
      val dataArray: Array[String] = data.split(",")
      SensorReading(dataArray(0).trim, dataArray(1).trim.toLong, dataArray(2).trim.toDouble).toString //转成String方便序列化输出
    })

    // Sink
    // 消费消息：/usr/local/kafka/bin/kafka-console-consumer.sh --bootstrap-server hadoop100:9092,hadoop101:9092,hadoop102:9092 --from-beginning --topic sinkTest
    dataStream.addSink(new FlinkKafkaProducer011[String]("hadoop100:9092,hadoop101:9092,hadoop102:9092", "sinkTest", new SimpleStringSchema()))
    // 控制台打印
    dataStream.print()

    env.execute("kafka sink test")
  }

}

package com.atguigu.apitest.sinktest

import com.atguigu.apitest.SensorReading
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}

object RedisSinkTest {

  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    // Source
    val inputStream: DataStream[String] = env.readTextFile("flink-demo/in/sensor.txt")

    // Transform
    val dataStream: DataStream[SensorReading] = inputStream.map(data => {
      val dataArray: Array[String] = data.split(",")
      SensorReading(dataArray(0).trim, dataArray(1).trim.toLong, dataArray(2).trim.toDouble)
    })

    val conf: FlinkJedisPoolConfig = new FlinkJedisPoolConfig.Builder()
      .setHost("hadoop100")
      .setPort(6379)
      .build()

    // Sink redis
    dataStream.addSink(new RedisSink(conf, new MyRedisMapper))

    env.execute("redis sink test")
  }

}

class MyRedisMapper() extends RedisMapper[SensorReading] {

  // 定义保存数据到redis的命令
  override def getCommandDescription: RedisCommandDescription = {
    // 把传感器id和温度值保存成哈希表 HSET key field value
    new RedisCommandDescription(RedisCommand.HSET, "sensor_temperature")
  }

  // 定义保存到redis的value
  override def getKeyFromData(t: SensorReading): String = t.temparature.toString

  // 定义保存到redis的key
  override def getValueFromData(t: SensorReading): String = t.id
}

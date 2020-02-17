package com.atguigu.apitest

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.functions.{AssignerWithPeriodicWatermarks, AssignerWithPunctuatedWatermarks}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time

object WindowTest {

  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.getConfig.setAutoWatermarkInterval(100L)

    //val streamFromFile: DataStream[String] = env.readTextFile("flink-demo/in/sensor.txt")
    val stream: DataStream[String] = env.socketTextStream("hadoop100", 7777)

    val dataStream: DataStream[SensorReading] = stream.map(data => {
      val dataArray: Array[String] = data.split(",")
      SensorReading(dataArray(0).trim, dataArray(1).trim.toLong, dataArray(2).trim.toDouble)
    })
      //      .assignAscendingTimestamps(_.timestamp * 1000)
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(1)) {
      override def extractTimestamp(t: SensorReading): Long = t.timestamp * 1000
    })

    /*
    // 统计10秒内的最小温度
    val minTempPerWindowStream: DataStream[(String, Double)] = dataStream
      .map(data => (data.id, data.temparature))
      .keyBy(_._1)
      .timeWindow(Time.seconds(10)) //开时间窗口
      .reduce((data1, data2) => (data1._1, data1._2.min(data2._2))) // 用reduce做增量集合
    */

    // 统计15秒内的最小温度，隔5秒输出一次
    val minTempPerWindowStream: DataStream[(String, Double)] = dataStream
      .map(data => (data.id, data.temparature))
      .keyBy(_._1)
      //.window(SlidingEventTimeWindows.of(Time.seconds(15), Time.seconds(5),Time.hours(-8)))
      .timeWindow(Time.seconds(15), Time.seconds(5)) //开时间窗口
      .reduce((data1, data2) => (data1._1, data1._2.min(data2._2))) // 用reduce做增量集合

    minTempPerWindowStream.print("min temp")
    dataStream.print("input data")

    env.execute("window test")

  }

}

//class MyAssigner() extends AssignerWithPeriodicWatermarks[SensorReading] {
//
//  val bound = 60000
//  var maxTs = Long.MinValue
//
//  override def getCurrentWatermark: Watermark = new Watermark(maxTs - bound)
//
//  override def extractTimestamp(t: SensorReading, l: Long): Long = {
//    maxTs = maxTs.max(t.timestamp * 1000)
//    t.timestamp * 1000
//  }
//}

class MyAssigner() extends AssignerWithPunctuatedWatermarks[SensorReading] {
  override def checkAndGetNextWatermark(t: SensorReading, l: Long): Watermark = new Watermark(l)

  override def extractTimestamp(t: SensorReading, l: Long): Long = t.timestamp * 1000
}

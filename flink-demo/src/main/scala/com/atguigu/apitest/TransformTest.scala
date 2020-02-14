package com.atguigu.apitest

import org.apache.flink.streaming.api.scala._

object TransformTest {

  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(1)

    val streamFromFile: DataStream[String] = env.readTextFile("flink-demo/in/sensor.txt")

    // 1.基本转换算子和简单聚合算子
    val dataStream: DataStream[SensorReading] = streamFromFile.map(data => {
      val dataArray: Array[String] = data.split(",")
      SensorReading(dataArray(0).trim, dataArray(1).trim.toLong, dataArray(2).trim.toDouble)
    })
    //      //.keyBy(0).sum(2)
    //    val aggStream: DataStream[SensorReading] = dataStream.keyBy("id")
    //      //.sum("temparature")
    //      // 输出当前传感器最新的温度+10，而时间戳是上一次数据的时间+1
    //      .reduce((x, y) => SensorReading(x.id, x.timestamp + 1, y.temparature + 10))
    //    aggStream.print()

    // 2.多流转换算子
    // 分流split
    val splitStream: SplitStream[SensorReading] = dataStream.split(data => {
      if (data.temparature > 30) Seq("high") else Seq("low")
    })
    val high: DataStream[SensorReading] = splitStream.select("high")
    val low: DataStream[SensorReading] = splitStream.select("low")
    val all: DataStream[SensorReading] = splitStream.select("high", "low")
    //    high.print("high")
    //    low.print("low")
    //    all.print("all")

    // 合并两条流
    // connect coMap
    //    val warning: DataStream[(String, Double)] = high.map(data => (data.id, data.temparature))
    //    val connectStream: ConnectedStreams[(String, Double), SensorReading] = warning.connect(low)
    //
    //    val coMapDataStream: DataStream[Product] = connectStream.map(
    //      warningData => (warningData._1, warningData._2, "warning"),
    //      lowData => (lowData.id, "healthy")
    //    )
    // coMapDataStream.print()

    // union 和connect的区别：1.connect只能操作两个流，union可以操作多个 2.union之前的两个流的类型必须一样，connect可以不一
    // 样，可以通过coMap调成一样
    val unionStream: DataStream[SensorReading] = high.union(low).union(all)
    unionStream.print()


    env.execute("transform test")

  }

}

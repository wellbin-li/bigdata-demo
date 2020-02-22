package com.atguigu.apitest

import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

object ProcessFunctionTest {

  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.enableCheckpointing(60000)

    val stream: DataStream[String] = env.socketTextStream("hadoop100", 7777)

    val dataStream: DataStream[SensorReading] = stream.map(data => {
      val dataArray: Array[String] = data.split(",")
      SensorReading(dataArray(0).trim, dataArray(1).trim.toLong, dataArray(2).trim.toDouble)
    })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(1)) {
        override def extractTimestamp(t: SensorReading): Long = t.timestamp * 1000
      })

    // val processedStream: DataStream[String] = dataStream.keyBy(_.id).process(new TempIncreAlter())

    val processedStream2: DataStream[(String, Double, Double)] = dataStream.keyBy(_.id)
      // .process(new TempChangeAlter2(10.0))
      .flatMap(new TempChangeAlter(10.0))

    val processedStream3: DataStream[(String, Double, Double)] = dataStream.keyBy(_.id)
      .flatMapWithState[(String, Double, Double), Double] {
      // 如果没有状态的话也就是没有数据来过，那么就将当前数据温度值存入状态
      case (input: SensorReading, None) => (List.empty, Some(input.temparature))
      // 如果有状态，就应该与上次的温度值比较差值，如果大于阈值就输出报警
      case (input: SensorReading, lastTemp: Some[Double]) => {
        val diff = (input.temparature - lastTemp.get).abs
        if (diff > 10.0) {
          (List((input.id, lastTemp.get, input.temparature)), Some(input.temparature))
        } else {
          (List.empty, Some(input.temparature))
        }
      }
    }

    dataStream.print("input data")
    processedStream3.print("processed data")

    env.execute("processfunction test")
  }

}

class TempIncreAlter() extends KeyedProcessFunction[String, SensorReading, String] {

  // 定义一个状态，用来保存上一个数据的温度值
  lazy val lastTemp: ValueState[Double] = getRuntimeContext.getState(new ValueStateDescriptor[Double]("lastTemp", classOf[Double]))
  // 定义一个状态，用来保存定时器的时间戳
  lazy val currentTimer: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("currentTimer", classOf[Long]))

  override def processElement(value: SensorReading, ctx: KeyedProcessFunction[String, SensorReading, String]#Context, out: Collector[String]): Unit = {
    // 先取出上一个温度值
    val preTemp: Double = lastTemp.value()
    // 更新温度值
    lastTemp.update(value.temparature)

    val curTimerTs = currentTimer.value()

    // 温度上升且没有设过定时器，则注册定时器
    if (value.temparature > preTemp && curTimerTs == 0) {
      val timerTs = ctx.timerService().currentProcessingTime() + 10000L
      ctx.timerService().registerProcessingTimeTimer(timerTs)
      currentTimer.update(timerTs)
    } else if (preTemp > value.temparature || preTemp == 0.0) {
      // 如果温度下降或是第一条数据，删除定时器并清空状态
      ctx.timerService().deleteProcessingTimeTimer(curTimerTs)
      currentTimer.clear()
    }
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, SensorReading, String]#OnTimerContext, out: Collector[String]): Unit = {
    // 输出报警信息
    out.collect(ctx.getCurrentKey + " 温度连续上升")
    currentTimer.clear()
  }

}

class TempChangeAlter(threshold: Double) extends RichFlatMapFunction[SensorReading, (String, Double, Double)] {

  private var lastTempState: ValueState[Double] = _

  override def open(parameters: Configuration): Unit = {
    // 初始化的时候声明state变量
    lastTempState = getRuntimeContext.getState(new ValueStateDescriptor[Double]("lastTemp", classOf[Double]))
  }

  override def flatMap(in: SensorReading, out: Collector[(String, Double, Double)]): Unit = {
    // 获取上次的温度值
    val lastTemp = lastTempState.value()
    // 用当前的温度值和上次的求差，如果大于阈值，输出报警信息
    val diff = (in.temparature - lastTemp).abs
    if (diff > threshold) {
      out.collect((in.id, lastTemp, in.temparature))
    }
    lastTempState.update(in.temparature)
  }
}

class TempChangeAlter2(threshold: Double) extends KeyedProcessFunction[String, SensorReading, (String, Double, Double)] {
  // 定义一个状态变量，保存上次的温度值
  lazy val lastTempState = getRuntimeContext.getState(new ValueStateDescriptor[Double]("lastTemp", classOf[Double]))

  override def processElement(value: SensorReading, ctx: KeyedProcessFunction[String, SensorReading, (String, Double, Double)]#Context, out: Collector[(String, Double, Double)]): Unit = {
    // 获取上次的温度值
    val lastTemp = lastTempState.value()
    // 用当前的温度值和上次的求差，如果大于阈值，输出报警信息
    val diff = (value.temparature - lastTemp).abs
    if (diff > threshold) {
      out.collect((value.id, lastTemp, value.temparature))
    }
    lastTempState.update(value.temparature)
  }

}
package com.atguigu.wc

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala._

// 流处理代码
object StreamWordCount {

  def main(args: Array[String]): Unit = {

    // 获取参数
    val param = ParameterTool.fromArgs(args)
    val host: String = param.get("host")
    val port: Int = param.getInt("port")

    // 创建一个流处理的执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    // env.setParallelism(1)
    // 设置task子任务不合并执行
    // env.disableOperatorChaining()

    // 接收socket数据流
    val textDataStream: DataStream[String] = env.socketTextStream(host, port)

    // 逐一读数据，分词之后进行wordcount
    val wordCountDataStream: DataStream[(String, Int)] = textDataStream.flatMap(_.split("\\s"))
      .filter(_.nonEmpty).startNewChain()
      .map((_, 1))
      .keyBy(0)
      .sum(1)

    // 打印输出
    wordCountDataStream.print().setParallelism(1)

    // 执行任务
    env.execute("stream word count job")

  }
}

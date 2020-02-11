package com.atguigu.wc

import org.apache.flink.api.scala._

// 批处理代码
object BatchWordCount {

  def main(args: Array[String]): Unit = {

    // 创建一个批处理的执行环境
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

    // 从文件中读取数据
    val inputDataSet: DataSet[String] = env.readTextFile("flink-demo/in/hello.txt")

    // 分词之后做count
    val wordCountDataSet: AggregateDataSet[(String, Int)] = inputDataSet.flatMap(_.split(" ")).map((_, 1)).groupBy(0).sum(1)

    // 打印输出
    wordCountDataSet.print()

  }

}

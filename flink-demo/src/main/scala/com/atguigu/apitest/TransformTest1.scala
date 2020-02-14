package com.atguigu.apitest

import org.apache.flink.streaming.api.scala._

object TransformTest1 {

  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    // map算子
    //    val stream = env.generateSequence(1,10)
    //    val streamMap = stream.map { x => x * 2 }
    //    streamMap.print()

    // flatMap算子
    //    val stream = env.readTextFile("flink-demo/in/hello.txt")
    //    val streamFlatMap = stream.flatMap{
    //      x => x.split(" ")
    //    }
    //    streamFlatMap.print()

    // filter算子
    //    val stream = env.generateSequence(1,10)
    //    val streamFilter = stream.filter{
    //      x => x == 1
    //    }
    //    streamFilter.print()

    // connect算子
    //    val stream = env.readTextFile("flink-demo/in/hello.txt")
    //    val streamMap = stream.flatMap(item => item.split(" ")).filter(item =>
    //      item.equals("hello"))
    //    val streamCollect = env.fromCollection(List(1,2,3,4))
    //    val streamConnect = streamMap.connect(streamCollect)
    //    streamConnect.map(item=>println(item), item=>println(item))

    // CoMap
    //    val stream1 = env.readTextFile("flink-demo/in/hello.txt")
    //    val streamFlatMap = stream1.flatMap(x => x.split(" "))
    //    val stream2 = env.fromCollection(List(1,2,3,4))
    //    val streamConnect = streamFlatMap.connect(stream2)
    //    val streamCoMap = streamConnect.map(
    //      (str) => str + "connect",
    //      (in) => in + 100
    //    )
    //    streamCoMap.print()

    // CoFlatMap
    //    val stream1 = env.readTextFile("flink-demo/in/word1.txt")
    //    val stream2 = env.readTextFile("flink-demo/in/word2.txt")
    //    val streamConnect = stream1.connect(stream2)
    //    val streamCoMap = streamConnect.flatMap(
    //      (str1) => str1.split(" "),
    //      (str2) => str2.split(" ")
    //    )
    //    streamConnect.map(item=>println(item), item=>println(item))

    // split算子
    //    val stream = env.readTextFile("flink-demo/in/word1.txt")
    //    val streamFlatMap = stream.flatMap(x => x.split(" "))
    //    val streamSplit = streamFlatMap.split(
    //      num =>
    //        // 字符串内容为 hadoop 的组成一个 DataStream，其余的组成一个 DataStream
    //        (num.equals("hadoop")) match {
    //          case true => List("hadoop")
    //          case false => List("other")
    //        }
    //    )

    // select算子
    //    val stream = env.readTextFile("flink-demo/in/word1.txt")
    //    val streamFlatMap = stream.flatMap(x => x.split(" "))
    //    val streamSplit = streamFlatMap.split(
    //      num =>
    //        (num.equals("hadoop")) match {
    //          case true => List("hadoop")
    //          case false => List("other")
    //        }
    //    )
    //    val hadoop = streamSplit.select("hadoop")
    //    val other = streamSplit.select("other")
    //    other.print()

    // union算子
    //    val stream1 = env.readTextFile("flink-demo/in/word1.txt")
    //    val streamFlatMap1 = stream1.flatMap(x => x.split(" "))
    //    val stream2 = env.readTextFile("flink-demo/in/word2.txt")
    //    val streamFlatMap2 = stream2.flatMap(x => x.split(" "))
    //    val streamConnect = streamFlatMap1.union(streamFlatMap2)
    //    streamConnect.print()

    // keyBy算子
    //    val stream = env.readTextFile("flink-demo/in/hello.txt")
    //    val streamFlatMap = stream.flatMap {
    //      x => x.split(" ")
    //    }
    //    val streamMap = streamFlatMap.map {
    //      x => (x, 1)
    //    }
    //    val streamKeyBy = streamMap.keyBy(0)
    //    streamKeyBy.print()

    // reduce算子
    //    val stream = env.readTextFile("flink-demo/in/hello.txt").flatMap(item => item.split(" ")).map(item =>
    //      (item, 1)).keyBy(0)
    //    val streamReduce = stream.reduce(
    //      (item1, item2) => (item1._1, item1._2 + item2._2)
    //    )
    //    streamReduce.print()

    // fold算子
    //    val stream = env.readTextFile("flink-demo/in/hello.txt").flatMap(item => item.split(" ")).map(item =>
    //      (item, 1)).keyBy(0)
    //    val streamReduce = stream.fold(100)(
    //      (begin, item) => (begin + item._2)
    //    )
    //    streamReduce.print()

    // Aggregations
    val stream = env.readTextFile("flink-demo/in/word3.txt").map(item => (item.split(" ")(0), item.split(" ")(1).toLong)).keyBy(0)
    val streamReduce = stream.sum(1)
    streamReduce.print()

    env.execute("transform test")

  }

}
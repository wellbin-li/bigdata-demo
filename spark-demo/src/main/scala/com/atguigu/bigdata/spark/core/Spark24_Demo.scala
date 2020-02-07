package com.atguigu.bigdata.spark.core

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 需求：统计出每一个省份广告被点击次数的 TOP3
  *
  */
object Spark24_Demo {
  def main(args: Array[String]): Unit = {

    //1.初始化 spark 配置信息并建立与 spark 的连接
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Test")
    val sc = new SparkContext(sparkConf)
    //2.读取数据生成 RDD： TS， Province， City， User， AD
    val line = sc.textFile("in/agent.log")
    //3.按照最小粒度聚合： ((Province,AD),1)
    val provinceAdAndOne = line.map { x =>
      val fields: Array[String] = x.split(" ")
      ((fields(1), fields(3)), 1)
    }
    //4.计算每个省中每个广告被点击的总数： ((Province,AD),sum)
    val provinceAdToSum = provinceAdAndOne.reduceByKey(_ + _)
    //5.将省份作为 key，广告加点击数为 value： (Province,(AD,sum))
    val provinceToAdSum = provinceAdToSum.map(x => (x._1._1, (x._1._2, x._2)))
    //6.将同一个省份的所有广告进行聚合(Province,List((AD1,sum1),(AD2,sum2)...))
    val provinceGroup = provinceToAdSum.groupByKey()
    //7.对同一个省份所有广告的集合进行排序并取前 3 条，排序规则为广告点击总数
    val provinceAdTop3 = provinceGroup.mapValues { x =>
      x.toList.sortWith((x, y) => x._2 > y._2).take(3)
    }
    //8.将数据拉取到 Driver 端并打印
    provinceAdTop3.collect().foreach(println)
    //9.关闭与 spark 的连接
    sc.stop()


  }

}
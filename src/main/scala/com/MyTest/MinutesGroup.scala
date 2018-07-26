package com.MyTest

import com.MyUtils.TimeGroup
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by zc_ch on 2018/4/16. 
  */
object MinutesGroup {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("MinutesGroup").setMaster("local[*]")
    //sparkContext，是Spark程序执行的入口
    val sc = new SparkContext(conf)

    //通过sc指定以后从哪里读取数据
    val lines: RDD[String] = sc.textFile("e:\\scalaIO.txt")

    //将内容切分  20170612101534,6
    val dateAndNum = lines.map(t => {
      val str = t.split(",")
      val num = str(1).toInt
      val groupByTime = TimeGroup.TimeTo5(str(0))

      (groupByTime, num)
    })
    //dateAndNum.foreach(println)

    //分组聚合
    val reduced: RDD[(String, Int)] = dateAndNum.reduceByKey(_ + _)

    //排序
    val sorted: RDD[(String, Int)] = reduced.sortBy(_._2, false)

    //保存结果
    val result: Array[(String, Int)] = sorted.collect()
    result.foreach(println)

    //释放资源
    sc.stop()
  }
}

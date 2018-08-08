package com.MyTest

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by zc_ch on 2018/8/7.
  */
object Different {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("IpLocation")
      .setMaster("local[*]")
      .set("spark.rdd.compress", "true")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer") // worker之间，rdd的序列化方式

    val sc = new SparkContext(conf)

    val line: RDD[String] = sc.textFile("E://a.txt")

    val pair = line.map(t => {
      val arr = t.split("[\t]",-1)
      val x = arr(0)
      val y = arr(2)
      if (!x.equals(y)) {
        (x, y)
      }
    })



    val list = pair.collect().toList
    list.foreach(println)
  }

}

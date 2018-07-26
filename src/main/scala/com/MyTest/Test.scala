package com.MyTest

import java.net.InetAddress
import java.text.SimpleDateFormat
import java.util
import java.util.{Calendar, Date}

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by zc_ch on 2018/4/24. 
  */
object Test {
  def main(args: Array[String]): Unit = {
    /*   val conf = new SparkConf().setAppName("Test").setMaster("local[*]")
       //sparkContext，是Spark程序执行的入口
       val sc = new SparkContext(conf)


       var rdd1 = sc.makeRDD(Array(("A", 1), ("A", 2),
         ("B", 1), ("B", 2), ("B", 3), ("B", 4),
         ("C", 1),
         ("D", 1), ("D", 2), ("D", 3),
         ("E", 1), ("E", 2), ("E", 3)
       ))
       rdd1.reduceByKey(_ + _).collect.foreach(println)
       println("*******************")
       rdd1.groupByKey().map(t => (t._1, t._2.sum)).collect.foreach(println)

       //释放资源
       sc.stop()

       */


    var arr = Array("a b c","b f a","a a c","c s f","a b a","c a b","a b a","b b","a f f","b a c","a c","c")

    val array: Array[String] = arr.flatMap(t =>t.split(" "))
    val wordAndNum: Array[(String, Int)] = array.map(t=>(t,1))
    val stringToTuples: Map[String, Array[(String, Int)]] = wordAndNum.groupBy(t=>t._1)
    val summed: Map[String, Int] = stringToTuples.mapValues(_.length)
    val sorted: List[(String, Int)] = summed.toList.sortBy(_._2)
    println(sorted)

  }

}

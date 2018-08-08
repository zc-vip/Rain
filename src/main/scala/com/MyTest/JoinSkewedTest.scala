package com.MyTest

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

import scala.util.Random

/**
  * Created by zc_ch on 2018/8/7.
  */
object JoinSkewedTest {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("Test").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val line1 = sc.parallelize(Array((1, "1"), (1, "11"), (1, "111"), (2, "2"), (3, "3"), (3, "33"), (0, "0")))
    val line2 = sc.parallelize(Array((1, "a"), (1, "1a"), (1, "1aa"), (2, "b"), (2, "2b"), (3, "c"), (0, "zero")))
    val value1: RDD[(Int, (String, String))] = line1.join(line2)

    val value2: RDD[(Int, (String, String))] = solution(line1, line2)
    value1.collect().foreach(println)
    println("********************")
    value2.collect().foreach(println)
    sc.stop()
  }

  /**
    * 采样倾斜key并分拆join操作
    *
    * @param pairs1
    * @param pairs2
    * @return
    */
  def solution(pairs1: RDD[(Int, String)], pairs2: RDD[(Int, String)]): RDD[(Int, (String, String))] = {
    val skewedKey: Int = pairs1.
       sample(false, 0.1).  //生产环境，数据量大，开启采样
      map(t => (t._1, 1)).reduceByKey((vl, v2) => vl + v2).
      map(t => t.swap).sortByKey(false).first._2
    // 从pairs1中分拆出导致数据倾斜的key，形成独立的RDD。
    val skewedRDD: RDD[(Int, String)] = pairs1.filter(_._1.equals(skewedKey))
    // 从pairs1中分拆出不导致数据倾斜的普通key，形成独立的RDD。
    val commonRDD: RDD[(Int, String)] = pairs1.filter(!_._1.equals(skewedKey))
    val SPLIT = "_"
    val size = 100
    // pairs2，就是那个所有key的分布相对较为均匀的rdd。
    // 这里将pairs2中，前面获取到的key对应的数据，过滤出来，分拆成单独的rdd，并对rdd中的数据使用flatMap算子都扩容100倍。
    // 对扩容的每条数据，都打上0～100的前缀。
    val skewedRDD2: RDD[(String, String)] = pairs2.filter(_._1.equals(skewedKey)).flatMap(t => {
      val array = new Array[(String, String)](size)
      for (i <- 0 until size) {
        array(i) = (i + SPLIT + t._1, t._2.toString)
      }
      array
    })
    val joinedRDDl = skewedRDD.map(t => {
      val prefix = new Random().nextInt(size)
      (prefix + SPLIT + t._1, t._2)
    }).join(skewedRDD2).map(t => {
      val key = t._1.split(SPLIT)(1).toInt
      (key, t._2)
    })
    val joinedRDD2: RDD[(Int, (String, String))] = commonRDD.join(pairs2)
    joinedRDDl.union(joinedRDD2)
  }


}

package com.MyTest

import com.Bean.IpBean
import com.MyUtils.IP_Tool
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by zc_ch on 2018/5/4. 
  */
object IpLocation {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("IpLocation")
      .setMaster("local[*]")
      .set("spark.rdd.compress", "true")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer") // worker之间，rdd的序列化方式
     // .registerKryoClasses(Array(classOf[IpBean])) // 自定义类必须手动的注册Kryo序列化方式

    val sc = new SparkContext(conf)
    //读取IP规则
    val ipRulesLine: RDD[String] = sc.textFile("E://ipRule.txt")

    //整理ip规则
    val IpRuldRdd: RDD[IpBean] = ipRulesLine.map(line => {
      val fields = line.split("[|]")
      val startIp = fields(0)
      val endIp = fields(1)
      val startNum = fields(2).toLong
      val endNum = fields(3).toLong
      val continent = fields(4)
      val country = fields(5)
      val province = fields(6)
      val city = fields(7)
      val operator = fields(8)

      new IpBean(startIp, endIp, startNum, endNum, continent, country, province, city, operator)
    })

    val IpRule = IpRuldRdd.collect()
    //将全部的IP规则通过广播的方式发生到Executor
    //广播之后，在Driver端获取了到了广播变量的引用(如果没有广播完，就不往下走)
    val broadcast: Broadcast[Array[IpBean]] = sc.broadcast(IpRule)

    //读取访问日志
    val accessLogLine: RDD[String] = sc.textFile("E://IpTest.txt")
    //整理访问日志
    val provinceAndNum = accessLogLine.map(line => {
      val fields = line.split("[|]")
      val ip = fields(1)
      val ipNum = IP_Tool.ip2Long(ip)
      //通过广播变量的引用获取到Executor中的全部IP规则，然后进行匹配ip规则
      val allIpRule: Array[IpBean] = broadcast.value
      //根据规则进行查找，（用二分法进行查找）
      var province = "未知"


      val num = IP_Tool.binarySearch(allIpRule, ipNum)
      if (num != -1) {
        province = allIpRule(num).province
      }

      (province, 1)
    })
    val result = provinceAndNum.reduceByKey(_ + _).collect()
    println(result.toBuffer)


  }





}

package com.MyTest

import com.MyUtils.IP_Tool

import scala.io.Source

object ForTest {
  def main(args: Array[String]): Unit = {
    val file=Source.fromFile("E:\\ip0.txt")
    for (line <-file.getLines()) {

      val ipNum: Long = IP_Tool.ip2Long(line)
      println(ipNum)
    }

    val ipLong: Array[Long] = Array(16793599,167773121,2755008534L)

    for (i<-ipLong){
      val str = IP_Tool.long2Ip(i)
      println(str)
    }

   /* for (num<-ipLong){
      val longs: immutable.IndexedSeq[Long] = (0 to 3).map(i ⇒ {
        val i2: Int = i

        (num >> i2 * 8) & 0xFF}
      ).reverse
     println(longs.mkString("."))
    }*/


  }
}

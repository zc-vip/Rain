package com.MyUtils

import com.Bean.IpBean

import scala.collection.mutable.ArrayBuffer

/**
  * Created by zc_ch on 2018/4/19. 
  */
object IP_Tool {
  /**
    *
    * @param ip
    * String类型的ip
    * @return
    * Long类型的ip
    */
  def ip2Long(ip: String): Long = {
    val fragments = ip.split("[.]")
    var ipNum = 0L
    for (i <- 0 until fragments.length) {
      ipNum = fragments(i).toLong | ipNum << 8L //8进制变为10进制
    }
    ipNum
  }

  def long2Ip(num: Long) = {
    s"${(0 to 3).map(i ⇒ (num >> i * 8) & 0xFF).reverse.mkString(".")}"
  }

  /*def numberToIp(number: Long): String = {
    var n = number
    val arr = ArrayBuffer[Long]()
    for (i <- 1 to 4) {
      val ni = n & 0xFF
      arr += ni
      n >>= 8
    }
    val sb = new StringBuilder
    for (s <- arr.reverse) {
      sb.append(s).append(".")
    }
    sb.deleteCharAt(sb.length - 1)
    sb.toString
  }*/

  def binarySearch(allIpRule: Array[IpBean], ipNum: Long): Int = {
    var low = 0
    var high = allIpRule.length - 1
    while (low <= high) {
      val middle = (low + high) / 2
      val midRule = allIpRule(middle)
      ipNum match {
        case _ if ipNum >= midRule.startNum && ipNum <= allIpRule(middle).endNum => return middle

        case _ if ipNum < midRule.startNum => high = middle - 1

        case _ if ipNum > midRule.startNum => low = middle + 1
      }
    }

    -1
  }



}

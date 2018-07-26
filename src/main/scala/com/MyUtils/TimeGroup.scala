package com.MyUtils

/**
  * Created by zc_ch on 2018/4/17. 
  */
object TimeGroup {
  /**
    *
    * @param date
    *           年月日时分秒：20180101122015
    * @return
    *         5分钟的一个粒度 201801011220---201801011225
    */
  def TimeTo5(date: String): String = {
    val minute = date.substring(10, 12).toLong
    var time_B = 0L

    val time = (date.substring(0, 12).toLong) / 5 * 5
    val time_A = time
    if (minute >= 55) {
      val day = date.substring(0, 8).toLong
      val hour = date.substring(8, 10).toLong
      if (hour == 23) {
        time_B = (day + 1) * 10000
      } else {
        time_B = day * 10000 + (hour + 1) * 100
      }
    } else {
      time_B = (time + 5)
    }
    val dateGroup: String = time_A + "---" + time_B
    dateGroup
  }
}

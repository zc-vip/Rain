package com.Bean

/**
  * Created by zc_ch on 2018/5/4. 
  */
case class IpBean(
              // startNum, endNum, province
              //  1.0.1.0|1.0.3.255|16777472|16778239|亚洲|中国|福建|福州||电信|350100|China|CN|119.306239|26.075302
              startIp: String,
              endIp: String,
              startNum: Long,
              endNum: Long,
              continent: String,
              country: String,
              province: String,
              city: String,
              operator: String
            ){

}



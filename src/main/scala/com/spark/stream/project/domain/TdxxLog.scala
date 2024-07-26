package com.spark.stream.project.domain

/**
  * @program: scala
  * @Date: 2022/3/28 15:10
  * @Author: Mr.Xie
  * @Description:
  */
/**
  * 封装清洗后的数据
  * @param hh 户号
  * @param dq 地区
  * @param startTime 送电时间
  * @param stopTime 停电时间
  * @param statusCode 是否延迟
  */
case class TdxxLog(hh:String,dq:String,stopTime:String,startTime:String,statusCode:Int)

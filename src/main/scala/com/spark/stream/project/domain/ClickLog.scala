package com.spark.stream.project.domain

/**
  * @program: scala
  * @Date: 2022/3/9 10:11
  * @Author: Mr.Xie
  * @Description:
  */
/**
  * 封装清洗后的数据
  * @param ip 日志访问的ip地址
  * @param time 日志访问的时间
  * @param courseId 日志访问的实战课程编号
  * @param statusCode 日志访问的状态码
  * @param referer 日志访问的referer信息
  */
case class ClickLog (ip:String,time:String,courseId:Int,statusCode:Int,referer:String)



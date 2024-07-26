package com.spark.stream.project.domain

/**
  * @program: scala
  * @Date: 2022/3/9 10:16
  * @Author: Mr.Xie
  * @Description:
  */
/**
  * 封装实战课程的总点击数结果
  * @param day_course 对应于Hbase中的RowKey
  * @param click_count 总点击数
  */
case class CourseClickCount(day_course:String,click_count:Int)


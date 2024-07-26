package com.spark.stream.project.domain

/**
  * @program: scala
  * @Date: 2022/3/9 10:21
  * @Author: Mr.Xie
  * @Description:
  */
/**
  * 封装统计通过搜索引擎多来的实战课程的点击量
  * @param day_serach_course 当天通过某搜索引擎过来的实战课程
  * @param click_count 点击数
  */
case class CourseSearchClickCount(day_serach_course:String,click_count:Int)


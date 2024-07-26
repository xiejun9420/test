package com.spark.stream.project.utils

import java.util.{Calendar, Date}

import org.apache.commons.lang3.time.FastDateFormat

/**
  * @program: scala
  * @Date: 2022/3/9 10:06
  * @Author: Mr.Xie
  * @Description:
  */
object DateUtils {
  //指定输入的日期格式
  val YYYYMMDDHMMSS_FORMAT= FastDateFormat.getInstance("yyyy-MM-dd hh:mm:ss")
  //指定输出格式
  val TARGET_FORMAT = FastDateFormat.getInstance("yyyyMMddhhmmss")

  //输入String返回该格式转为log的结果
  def getTime(time:String) = {
    YYYYMMDDHMMSS_FORMAT.parse(time).getTime
  }

  def parseToMinute(time:String) = {
    //调用getTime
    TARGET_FORMAT.format(getTime(time))
  }

  def normalTime(stopTime:String,startTime:String) : Boolean = {
    val start = YYYYMMDDHMMSS_FORMAT.parse(stopTime)
    val end = YYYYMMDDHMMSS_FORMAT.parse(startTime)
    if((end.getTime -  start.getTime) > 0)
      true
    else
      false
  }

  def countTimeDiff(stopTime:String,startTime:String) : Double = {
    var d = 0.0
    val start = TARGET_FORMAT.parse(stopTime)
    val end = TARGET_FORMAT.parse(startTime)
    val cal1 = Calendar.getInstance()
    val cal2 = Calendar.getInstance()
    cal1.setTime(start)
    cal2.setTime(end)
    if(cal1.get(Calendar.DAY_OF_YEAR) == cal2.get(Calendar.DAY_OF_YEAR)){
      d = this.sameDay(start,end)
    }else{
      if((end.getTime - start.getTime)/86400000 < 1){
        d = this.diffDateLessDay(start,end)
      }else{
        var day = (end.getTime - start.getTime)/86400000
        cal1.add(Calendar.DAY_OF_MONTH,day.toInt)
        if(cal1.get(Calendar.DAY_OF_YEAR) == cal2.get(Calendar.DAY_OF_YEAR)){
          d = this.sameDay(cal1.getTime,end)
        }else{
          d = this.diffDateLessDay(cal1.getTime,end)
        }
        d = day * 1.0 + d
      }
    }
    d
  }

  def sameDay(stopTime: Date, startTime: Date) : Double = {
    var d = 0.0
    var hours1 = 0.0
    var hours2 = 0.0
    //停电时间0-6
    if(stopTime.getHours >= 0 && stopTime.getHours < 6){
      //送电时间0-6
      if(startTime.getHours >= 0 && startTime.getHours < 6){
        d = (startTime.getTime - stopTime.getTime)/(3600000.00)/8*0.35
      }else if (startTime.getHours >= 6 && startTime.getHours < 22){
        //送电时间6-22
        hours1 = ((5 - stopTime.getHours)*3600 + (59- stopTime.getMinutes)*60 + (60 - stopTime.getSeconds))/3600.00
        hours2 = ((startTime.getHours - 6)*3600 + startTime.getMinutes*60 + startTime.getSeconds)/3600.00
        d = hours1/8*0.35 + hours2/16*0.65
      }else{
        //送电时间22-24
        hours1 = ((5 - stopTime.getHours)*3600 + (59- stopTime.getMinutes)*60 + (60 - stopTime.getSeconds))/3600.00
        hours2 = ((startTime.getHours - 22)*3600 + startTime.getMinutes*60 + startTime.getSeconds)/3600.00
        d = (hours1 + hours2)/8*0.35
      }
    }else if (stopTime.getHours >= 6 && stopTime.getHours < 22){
      //停电时间6-22
      //因为是同一天，送电时间大于停电时间
      if(startTime.getHours >= 6 && startTime.getHours < 22){
        //送电时间6-22
        d = (startTime.getTime - stopTime.getTime)/(3600000.00)/16*0.65
      }else{
        //送电时间22-24
        hours1 = ((21 - stopTime.getHours)*3600 + (59- stopTime.getMinutes)*60 + (60 - stopTime.getSeconds))/3600.00
        hours2 = ((startTime.getHours - 22)*3600 + startTime.getMinutes*60 + startTime.getSeconds)/3600.00
        d = hours1/16*0.65 + hours2/8*0.35
      }
    }else{
      //停电时间22-24
      //因为是同一天，送电时间大于停电时间
      d = (startTime.getTime - stopTime.getTime)/3600000.00/8*0.35
    }
    d
  }

  def diffDateLessDay (stopTime: Date, startTime: Date) : Double = {
    var d = 0.0
    var dtemp = 0.0
    var hours1 = 0.0
    var hours2 = 0.0
    //存在隔天，但是不满24小时
    //停电时间0-6
    if(stopTime.getHours >= 0 && stopTime.getHours < 6){
      //隔间时间不满24小时
      d = 1 - (86400000 - (startTime.getTime - stopTime.getTime))/3600000.00/8*0.35
    }else if (stopTime.getHours >= 6 && stopTime.getHours < 22){
      //停电时间6-22
      if(startTime.getHours >= 0 && startTime.getHours < 6){
        //送电时间0-6
        hours1 = ((21 - stopTime.getHours)*3600 + (59- stopTime.getMinutes)*60 + (60 - stopTime.getSeconds))/3600.00
        hours2 = (startTime.getHours*3600 + startTime.getMinutes*60 + startTime.getSeconds)/3600.00
        d = hours1/16*0.65 + (2 + hours2)/8*0.35
      }else{
        //送电时间6-22
        d = 1 - (86400000 -(startTime.getTime - stopTime.getTime))/3600000.00/16*0.65
      }
    }else{
      //停电时间22-24
      //送电时间0-6
      if(startTime.getHours >= 0 && startTime.getHours < 6){
        hours1 = ((23 - stopTime.getHours)*3600 + (59- stopTime.getMinutes)*60 + (60 - stopTime.getSeconds))/3600.00
        hours2 = (startTime.getHours*3600 + startTime.getMinutes*60 + startTime.getSeconds)/3600.00
        d = (hours1 + hours2)/8*0.35
      }else if (startTime.getHours >= 6 && startTime.getHours < 22){
        //送电时间6-22
        hours1 = ((23 - stopTime.getHours + 6)*3600 + (59 - stopTime.getMinutes)*60 + (60 - stopTime.getSeconds))/3600.00
        hours2 = ((startTime.getHours - 6)*3600 + startTime.getMinutes*60 + startTime.getSeconds)/3600.00
        d = (hours1 + 6)/8*0.35 + hours2/16*0.65
      }else{
        //送电时间22-24
        d = 1 - (86400000 -(startTime.getTime - stopTime.getTime))/3600000.00/8*0.35
      }
    }
    d
  }
}

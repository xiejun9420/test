package com.spark.stream.project.dao

import com.spark.stream.project.domain.{DqStopPowerCount, HhStopPowerCount}
import com.spark.stream.project.utils.HBaseUtils
import org.apache.hadoop.hbase.util.Bytes

import scala.collection.mutable.ListBuffer

/**
  * @program: scala
  * @Date: 2022/3/28 16:21
  * @Author: Mr.Xie
  * @Description:
  */
object DqStopCountDao {

  val tableName = "ns1:dq_stopcount"  //表名
  val cf = "info"   //列族
  val qualifer = "stop_count"   //列

  /**
    * 保存数据到Hbase
    * @param list (day_course:String,click_count:Int) //统计后当天每门课程的总点击数
    */
  def save(list:ListBuffer[DqStopPowerCount]): Unit = {
    //调用HBaseUtils的方法，获得HBase表实例
    val table = HBaseUtils.getInstance().getTable(tableName)
    for(item <- list){
      //调用Hbase的一个自增加方法
      table.incrementColumnValue(Bytes.toBytes(item.day_dq),
        Bytes.toBytes(cf),
        Bytes.toBytes(qualifer),
        item.stop_count)  //赋值为Long,自动转换
    }
  }

}

package com.spark.stream.project.application

import com.spark.stream.project.dao.{DqStopCountDao, HhStopCountDao, TdscxsCountDao}
import com.spark.stream.project.domain.{DqStopPowerCount, HhStopPowerCount, Tdscxs, TdxxLog}
import com.spark.stream.project.utils.DateUtils
import kafka.serializer.StringDecoder
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable.ListBuffer

/**
  * @program: scala
  * @Date: 2022/3/28 10:07
  * @Author: Mr.Xie
  * @Description:停电信息分析
  */
object AnalyseTdxxByStreaming {

  val shutdownMarker = "/tmp/spark-test/stop-spark"
  var stopFlag: Boolean = false

  def main(args: Array[String]): Unit = {
    //kafka direct模式获取数据
    if (args.length != 2) {
      System.err.println("Usage:KafkaDirectWordCount <brokers> <topics>")
      System.exit(1)
    }

    val Array(brokerList, topicStr) = args
    //direct中的参数为一个set集合
    val topics = topicStr.split(",").toSet
    val conf = new SparkConf()
      .setAppName("AnalyseTdxxByStreaming")
      //没有Receiver这里给一个就行
      .setMaster("local[2]")
      //设置spark程序每秒中从每个partition分区读取的最大的数据条数
      .set("spark.streaming.kafka.maxRatePerPartition", "100")

    val batchInterval = Seconds(60)
    val kafkaParams: Map[String, String] = Map[String, String](
      "metadata.broker.list" -> brokerList,
       ConsumerConfig.GROUP_ID_CONFIG -> "test",
      "auto.offset.reset" -> "smallest"
    )
    val ssc = new StreamingContext(conf, batchInterval)
    val input: InputDStream[(String, String)] = KafkaUtils
      .createDirectStream[String, String, StringDecoder, StringDecoder](ssc,
      kafkaParams, topics)

    //得到原始的日志数据
    val logResourcesDS = input.map(_._2)

    /**
      * (1)清洗数据,过滤掉非法的数据
      */
    val cleanDataRDD = logResourcesDS.map(line => {
      val splits = line.split("\t")
      if(splits.length != 5) {      //不合法的数据直接封装默认赋予错误值，filter会将其过滤
        TdxxLog("", "", "", "", 2)
      }
      else {
        val id = splits(0)   //获得日志中用户的id
        val dq = splits(1)   //获得日志中的地区
        val start = splits(2)
        val end = splits(3)
        if(DateUtils.normalTime(start,end)){
          val diff = DateUtils.getTime(end) - DateUtils.getTime(start)
          if(diff >= 180000){
            val time1 = DateUtils.parseToMinute(splits(2)) //获得日志中停电时间，并调用DateUtils格式化时间
            val time2 = DateUtils.parseToMinute(splits(3)) //获得日志中送电时间，并调用DateUtils格式化时间
            val status = splits(4).toInt  //获得是否超时停电状态标志
            TdxxLog(id,dq,time1,time2,status)  //将清洗后的日志封装到TdxxLog中
          }else{
            TdxxLog("", "", "", "", 2)
          }
        }else{
          TdxxLog("", "", "", "", 2)
        }
      }
    }).filter(x => x.statusCode != 2 )   //过滤掉非法数据

    /**
      * (1)统计数据
      * (2)把计算结果写进HBase
      */
    cleanDataRDD .map(line => {
      // 将‘年_户号’作为RowKey,意义为某年某户的停电次数
      (line.stopTime.substring(0,4) + "_" + line.hh,1)   //映射为元组
    }).reduceByKey(_ + _)   //聚合
      .foreachRDD(rdd =>{    //一个DStream里有多个RDD
      rdd.foreachPartition(partition => {   //一个RDD里有多个Partition
        val list = new ListBuffer[HhStopPowerCount]
        partition.foreach(item => {   //一个Partition里有多条记录
          list.append(HhStopPowerCount(item._1,item._2))
        })
        HhStopCountDao.save(list)   //保存至HBase
      })
    })

    /**
      * 统计某年某地区的总停电次数
      * (1)统计数据
      * (2)把统计结果写进HBase中去
      */
    cleanDataRDD .map(line => {
      // 将‘日期_地区’作为RowKey,意义为某天某地区的停电次数
      (line.stopTime.substring(0,4) + "_" + line.dq,1)   //映射为元组
    }).reduceByKey(_ + _)   //聚合
      .foreachRDD(rdd =>{    //一个DStream里有多个RDD
      rdd.foreachPartition(partition => {   //一个RDD里有多个Partition
        val list = new ListBuffer[DqStopPowerCount]
        partition.foreach(item => {   //一个Partition里有多条记录
          list.append(DqStopPowerCount(item._1,item._2))
        })
        DqStopCountDao.save(list)   //保存至HBase
      })
    })

    /**
      * 统计某日某用户的停电时长系数数
      * (1)统计数据
      * (2)把统计结果写进HBase中去
      */
    cleanDataRDD .map(line => {
      // 将‘日期_地区’作为RowKey,意义为某天某地区的停电时长系数
      (line.stopTime.substring(0,8) + "_" + line.hh,DateUtils.countTimeDiff(line.stopTime,line.startTime))   //映射为元组
    }).reduceByKey(_ + _)   //聚合
      .foreachRDD(rdd =>{    //一个DStream里有多个RDD
      rdd.foreachPartition(partition => {   //一个RDD里有多个Partition
        val list = new ListBuffer[Tdscxs]
        partition.foreach(item => {   //一个Partition里有多条记录
          list.append(Tdscxs(item._1,item._2))
        })
        TdscxsCountDao.save(list)   //保存至HBase
      })
    })

    ssc.start()
    //ssc.awaitTermination()
    val checkIntervalMillis = 10000
    var isStopped = false
    while (!isStopped) {
      println("calling awaitTerminationOrTimeout")
      //等待执行停止。执行过程中发生的任何异常都会在此线程中抛出，如果执行停止了返回true，
      //线程等待超时长，当超过timeout时间后，会监测ExecutorService是否已经关闭，若关闭则返回true，否则返回false。
      isStopped = ssc.awaitTerminationOrTimeout(checkIntervalMillis)
      if (isStopped) {
        println("confirmed! The streaming context is stopped. Exiting application...")
      } else {
        println("Streaming App is still running. Timeout...")
      }
      //判断文件夹是否存在
      checkShutdownMarker
      if (!isStopped && stopFlag) {
        println("stopping ssc right now")
        //第一个true：停止相关的SparkContext。无论这个流媒体上下文是否已经启动，底层的SparkContext都将被停止。
        //第二个true：则通过等待所有接收到的数据的处理完成，从而优雅地停止。
        ssc.stop(true, true)
        println("ssc is stopped!!!!!!!")
      }
    }
  }

  def checkShutdownMarker = {
    if (!stopFlag) {
      //开始检查hdfs是否有stop-spark文件夹
      val fs = FileSystem.get(new Configuration())
      //如果有返回true，如果没有返回false
      stopFlag = fs.exists(new Path(shutdownMarker))
    }
  }
}

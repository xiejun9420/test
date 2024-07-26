package com.spark.stream.project.application

import com.spark.stream.project.utils.DateUtils
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @program: scala
  * @Date: 2023/9/5 16:35
  * @Author: Mr.Xie
  * @Description:
  */
object StopPoweAnalyseOffLine {


  def main(args: Array[String]): Unit = {

    /**
      * 最终该程序将打包在集群上运行，
      * 需要接收参数 inputPath : 文件在hdfs上的路径（数据来源）
      */
    if (args.length != 1) {
      System.err.println("Error:you need to input:<inputPath>")
      System.exit(1)
    }

    //接收main函数的参数，外面的传参
    val Array(inputPath) = args

    /**
      * 创建Spark上下文，下本地运行需要设置AppName
      * Master等属性，打包上集群前需要删除
      */
    val sparkConf = new SparkConf()
      .setAppName("StopPoweAnalyseOffLine")

    val sc = new SparkContext(sparkConf)
    //读取hdfs文件
    //val rdd = sc.textFile("hdfs://node01:/user/root/tdsj.txt")
    val rdd = sc.textFile(inputPath)
    val result = rdd.map(line => {
      val item = line.split("\t")
      // 将‘日期_地区’作为RowKey,意义为某天某地区的停电时长系数
      (item(4).substring(0, 8) + "_" + item(1), DateUtils.countTimeDiff(item(4), item(5))) //映射为元组
    }).reduceByKey(_ + _) //聚合


    sc.stop()
  }

}

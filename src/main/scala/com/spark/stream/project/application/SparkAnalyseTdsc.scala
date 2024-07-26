package com.spark.stream.project.application

import com.spark.stream.project.utils.DateUtils
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @program: scala
  * @Date: 2023/7/10 14:44
  * @Author: Mr.Xie
  * @Description:
  */
object SparkAnalyseTdsc {

  def main(args: Array[String]): Unit = {

    /**
      * 最终该程序将打包在集群上运行，
      * 需要接收几个参数：zookeeper服务器的ip，kafka消费组，
      * 主题，以及线程数
      */
    if (args.length != 2) {
      System.err.println("Error:you need to input:<inputPath> <outputPath>")
      System.exit(1)
    }

    //接收main函数的参数，外面的传参
    val Array(inputPath, outputPath) = args

    /**
      * 创建Spark上下文，下本地运行需要设置AppName
      * Master等属性，打包上集群前需要删除
      */
    val sparkConf = new SparkConf()
      .setAppName("SparkAnalyseTdsc")

    val sc = new SparkContext(sparkConf)
    //读取hdfs文件
    //val rdd = sc.textFile("hdfs://node01:/user/root/tdsj.txt")
    val rdd = sc.textFile(inputPath)
    val result = rdd.map(line => {
      val item = line.split("\t")
      // 将‘日期_地区’作为RowKey,意义为某天某地区的停电时长系数
      (item(4).substring(0, 8) + "_" + item(1), DateUtils.countTimeDiff(item(4), item(5))) //映射为元组
    }).reduceByKey(_ + _) //聚合

    result.saveAsTextFile(outputPath)
    sc.stop()
  }
}

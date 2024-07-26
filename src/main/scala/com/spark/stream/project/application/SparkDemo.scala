package com.spark.stream.project.application

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @program: scala
  * @Date: 2023/7/6 17:09
  * @Author: Mr.Xie
  * @Description:
  */
object SparkDemo {

  def main(args: Array[String]): Unit = {

    /**
      * 最终该程序将打包在集群上运行，
      * 需要接收几个参数：zookeeper服务器的ip，kafka消费组，
      * 主题，以及线程数
      */
    /*if(args.length != 2){
      System.err.println("Error:you need to input:<inputPath> <outputPath>")
      System.exit(1)
    }*/

    //接收main函数的参数，外面的传参
    //val Array(inputPath,outputPath) = args

    /**
      * 创建Spark上下文，下本地运行需要设置AppName
      * Master等属性，打包上集群前需要删除
      */
    val sparkConf = new SparkConf()
      .setAppName("SparkDemo")

    val sc = new SparkContext(sparkConf)
    //读取hdfs文件
    val rdd = sc.textFile("hdfs://node01:/user/root/tdsj.txt")
    val result = rdd
      .map(line => {
        (line.split("\t")(3), 1)
      })
      .reduceByKey(_ + _)
    val sortAdd = result.map(pair => (pair._2,pair._1)).sortByKey(false).map(pair => (pair._2,pair._1))
    sortAdd.collect().foreach(println)
/*    result.map(x => {
      x._1 + "\t" + x._2
    })
      .saveAsTextFile("hdfs://node01:/output/tdsj/")*/

    //sc.stop()
  }

}

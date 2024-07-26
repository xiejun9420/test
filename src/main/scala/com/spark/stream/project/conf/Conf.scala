package com.spark.stream.project.conf

/**
  * @program: scala
  * @Date: 2022/3/28 10:47
  * @Author: Mr.Xie
  * @Description:
  */
object Conf {

  val ZK_QUORUM = "node02:2181,node03:2181,node04:2181";
  val TOPIC_NAME = "tdxxtopic";
  val GROUP_NAME = "test";
  val THREAD_NUM = 1;
}

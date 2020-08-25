package com.hainiu.sparkcore

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object CheckPointDemo {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[5]").setAppName("CheckPointDemo")
    val sc = new SparkContext(conf)
    //设置checkpoint目录
    sc.setCheckpointDir("/tmp/spark/check_point_demo_data")
    val rdd: RDD[Int] = sc.parallelize(1 to 3, 2)
    val rdd2: RDD[Int] = rdd.map(f => {
      println(s"f:${f}")
      f * 10
    })
    //checkpoint是转换算子
    val cp: Unit = rdd2.checkpoint()
//    rdd2.count()
  }
}

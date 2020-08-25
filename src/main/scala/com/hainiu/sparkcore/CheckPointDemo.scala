package com.hainiu.sparkcore

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object CheckPointDemo {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[5]").setAppName("CheckPointDemo")
    val sc = new SparkContext(conf)
    val rdd: RDD[Int] = sc.parallelize(1 to 3, 2)
    val rdd2: RDD[Int] = rdd.map(f => {
      println(s"f:${f}")
      f * 10
    })
    rdd2.count()
  }
}

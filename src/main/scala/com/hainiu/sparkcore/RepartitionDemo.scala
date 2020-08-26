package com.hainiu.sparkcore

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object RepartitionDemo {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[5]").setAppName("SparkDemo3")
    val sc = new SparkContext(conf)
    val rdd: RDD[Int] = sc.parallelize(1 to 10, 2)
    println(s"刚开始分区数：${rdd.getNumPartitions}")
    val pairRdd: RDD[(Int, Int)] = rdd.map(f => {
      println(s"f:${f}")
      (f, 1)
    })
    pairRdd.count()
  }
}

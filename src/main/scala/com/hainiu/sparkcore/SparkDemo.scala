package com.hainiu.sparkcore

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SparkDemo {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[5]").setAppName("SparkDemo")
    val sc = new SparkContext(conf)
    val rdd: RDD[Int] = sc.parallelize(1 to 10, 2)
    //map操作的是rdd里面的每个元素
    //    val arr1: Array[Int] = rdd.map(f => {
    //      println(s"f:${f} ---> ${f*10}")
    //      f * 10
    //    }).collect()
    //    println(arr1.toBuffer) 
    val rdd2: RDD[Int] = rdd.mapPartitionsWithIndex((index, it) => {
      val list1: List[Int] = it.toList
      println(s"${index}: ${list1}")
      val it2: List[Int] = list1.map(f => f * 10)
      it2.toIterator
    })
    val arr2: Array[Int] = rdd2.collect()
    println(arr2.toBuffer)
  }
}

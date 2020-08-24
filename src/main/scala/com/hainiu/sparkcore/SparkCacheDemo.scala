package com.hainiu.sparkcore

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SparkCacheDemo{
    
  def main(args: Array[String]): Unit = {
  //local[*]:利用cpu核数运算
  //local[N]:利用N个核数运算
  val sparkConf: SparkConf = new SparkConf().setAppName("SparkCacheDemo").setMaster("local[*]")
  val sc = new SparkContext(sparkConf)
  val rdd: RDD[String] = sc.textFile("H:\\input1")
    val rdd2: RDD[(String, Int)] = rdd.flatMap(_.split("\t")).map(
      f => {
        println(s"f:${f}")
        (f, 1)
      }
    )
    rdd2.count()
    rdd2.count()
  }
}

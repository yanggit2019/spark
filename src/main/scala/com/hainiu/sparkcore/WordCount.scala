package com.hainiu.sparkcore

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object WordCount {
  def main(args: Array[String]): Unit = {
    //local[*]:利用cpu核数运算
    //local[N]:利用N个核数运算
    val sparkConf: SparkConf = new SparkConf().setAppName("wordcount").setMaster("local[*]")
    val sc = new SparkContext(sparkConf)
    val rdd: RDD[String] = sc.textFile("H:\\input1")
    val resRdd: RDD[(String, Int)] = rdd.flatMap(_.split("\t")).map((_, 1)).groupBy(_._1).mapValues(_.size)
    val arr: Array[(String, Int)] = resRdd.collect()
    println(arr.toBuffer)
    
  }
}

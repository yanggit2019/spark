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
//    val resRdd: RDD[(String, Int)] = rdd.flatMap(_.split("\t")).map((_, 1)).groupBy(_._1).mapValues(_.size)
    //因为groupBy操作是按照key把value聚合在一起，但并不运算，这样拉取的数据量就很大
    //所以建议用reduceByKey,它是按照key进行聚合，并运算，那拉取的数据是已经计算完的结果，效率高
    val resRdd: RDD[(String, Int)] = rdd.flatMap(_.split("\t")).map((_, 1)).reduceByKey(_+_)
    val arr: Array[(String, Int)] = resRdd.collect()
    println(arr.toBuffer)
    
  }
}

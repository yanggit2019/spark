package com.hainiu.sparkcore

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SerDemo {
  def main(args: Array[String]): Unit = {
    val SparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SerDemo")
    //开启Kryo序列化
    SparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    //要求主动注册
    SparkConf.set("spark.kryo.registrationRequired","true")
   
    
    
    val sc = new SparkContext(SparkConf)
    val rdd: RDD[String] = sc.parallelize(List("aa", "aa", "bb", "aa"), 2)
    val broad: Broadcast[UserInfo] = sc.broadcast(new UserInfo)

    val pairRdd: RDD[(String, UserInfo)] = rdd.map(f => {
      val pairRdd: UserInfo = broad.value
      (f, pairRdd)
    })
    //因为groupByKey有shuffle,需要序列化
    val groupRdd: RDD[(String, Iterable[UserInfo])] = pairRdd.groupByKey()
    val arr: Array[(String, Iterable[UserInfo])] = groupRdd.collect()
    for (t <- arr){
      println(t)
    }
  }
}

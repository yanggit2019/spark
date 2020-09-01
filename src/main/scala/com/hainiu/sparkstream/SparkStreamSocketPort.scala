package com.hainiu.sparkstream

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Durations, StreamingContext}

object SparkStreamSocketPort {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("SparkStreamSocketPort").setMaster("local[*]")
    val streamingContext = new StreamingContext(conf, Durations.seconds(5))

    val lines: ReceiverInputDStream[String] = streamingContext.socketTextStream("192.168.1.51", 6666)
    lines.foreachRDD(r =>{
      if (!r.isEmpty()){
      val flatMap: RDD[String] = r.flatMap(_.split(" "))
      val mapToPair: RDD[(String, Int)] = flatMap.map((_, 1))
      val reduceByKey: RDD[(String, Int)] = mapToPair.reduceByKey(_ + _)
      println(s"${reduceByKey.collect().toList}")
      }
    })
//    val flatMap: DStream[String] = lines.flatMap(_.split(" "))
//    val reduceBykey: DStream[(String, Int)] = flatMap.transform(r => {
//      val mapToPair: RDD[(String, Int)] = r.map((_, 1))
//      val reduceByKey: RDD[(String, Int)] = mapToPair.reduceByKey(_ + _)
//      reduceByKey
//    })
////    val mapToPair: DStream[(String, Int)] = flatMap.map((_, 1))
////    val reduceByKey: DStream[(String, Int)] = mapToPair.reduceByKey(_ + _)
//
//    reduceBykey.foreachRDD( r=>{
//      if (!r.isEmpty()){
//      println(s"${r.collect().toList}")
//      }
//    })
    
    streamingContext.start()
    streamingContext.awaitTermination()
    
  }
}

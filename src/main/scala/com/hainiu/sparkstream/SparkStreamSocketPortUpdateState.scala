package com.hainiu.sparkstream

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Durations, StreamingContext}

object SparkStreamSocketPortUpdateState {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("SparkStreamSocketPort").setMaster("local[*]")
    val chcekPoint = "/tmp/checkpoin/SparkStreamSocketPortUpdateState"
    val context: StreamingContext = StreamingContext.getOrCreate(chcekPoint, () => {
      
      val streamingContext = new StreamingContext(conf, Durations.seconds(5))
          streamingContext.checkpoint(chcekPoint)
      val lines: ReceiverInputDStream[String] = streamingContext.socketTextStream("192.168.1.51", 6666)
      val flatMap: DStream[String] = lines.flatMap(_.split(" "))
      val mapToPair: DStream[(String, Int)] = flatMap.map((_, 1))
      val reduceByKey: DStream[(String, Int)] = mapToPair.reduceByKey(_ + _)
      val updateStateByKey: DStream[(String, Int)] = reduceByKey.updateStateByKey((a: Seq[Int], b: Option[Int]) => {
        var total = 0
        for (i <- a) {
          total += i
        }
        val last: Int = if (b.isDefined) b.get else 0
        val now: Int = last + total
        Some(now)
      })
      updateStateByKey.foreachRDD(r =>{
        if(!r.isEmpty()){
          println(s"${r.collect().toList}")
        }
      })
      streamingContext
    })
    


    context.start()
    context.awaitTermination()
  }
}

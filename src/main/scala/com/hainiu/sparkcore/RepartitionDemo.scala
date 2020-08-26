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
    //repartition是宽依赖，划分阶段会断开，会产生新阶段
    val rdd2: RDD[(Int, Int)] = pairRdd.repartition(5)
    println(s"增加分区后分区数:${rdd2.getNumPartitions}")
    //coalesce是窄依赖，不会产生新阶段
    val rdd3: RDD[(Int, Int)] = rdd2.coalesce(3)
    println(s"减少分区后分区数：${rdd3.getNumPartitions}")

    val rdd4: RDD[(Int, Int)] = rdd3.reduceByKey(_ + _,1)
    println(s"reduceByKey后分区数: ${rdd4.getNumPartitions}")
    println(rdd4.toDebugString)
    rdd4.count()
  }
}

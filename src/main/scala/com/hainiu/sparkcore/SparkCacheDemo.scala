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
    //因为cache是把rdd的数据存储到存储内存，当执行后续操作的时候，可以直接从存储内存拿取数据
    //进而达到数据的复用
    //cache 默认的缓存级别是StorageLevel.MEMORY_ONLY(完全放入内存)
    val cache: rdd2.type = rdd2.cache()
    cache.count()
    cache.count()
  }
}

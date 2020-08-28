package com.hainiu.sparkcore

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SortAndShuffleDemo {
  def main(args: Array[String]): Unit = {
    //local[*]:利用cpu核数运算
    //local[N]:利用N个核数运算
    val sparkConf: SparkConf = new SparkConf().setAppName("wordcount").setMaster("local[*]")
    val sc = new SparkContext(sparkConf)
    val rdd: RDD[String] = sc.textFile("H:\\input1")
    //    val resRdd: RDD[(String, Int)] = rdd.flatMap(_.split("\t")).map((_, 1)).groupBy(_._1).mapValues(_.size)
    //因为groupBy操作是按照key把value聚合在一起，但并不运算，这样拉取的数据量就很大
    //所以建议用reduceByKey,它是按照key进行聚合，并运算，那拉取的数据是已经计算完的结果，效率高
    val resRdd: RDD[(String, Int)] = rdd.flatMap(_.split("\t")).map((_, 1)).reduceByKey(_+_,2)
    //    val arr: Array[(String, Int)] = resRdd.collect()
    //    println(arr.toBuffer)
    //将内容写入文件
    val res2: RDD[(String, Int)] = resRdd.sortBy(_._2, false)
    //打印rdd的依赖关系
    println(res2.toDebugString)
    val outputDir = "/tmp/spark/output"
    //引入隐式转换函数实现给字符串赋予能删除hdfs的功能
    import com.hainiu.util.MyPredef.string2HDFSUtil
    outputDir.deleteHdfs()
    res2.saveAsTextFile(outputDir)
 
  }
  
}

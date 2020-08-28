package com.hainiu.sparkcore

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
//自定义二次比较的key
//按照单词降序，数值升序
class SecondaryKey(val word:String,val num:Int) extends Ordered[SecondaryKey] with Serializable {
  override def compare(that: SecondaryKey): Int = {
    //单词相同，按照数值升序排序
    if(this.word.compareTo(that.word) ==0){
      this.num - that.num
    }else{
      //按照单词降序排序
      that.word.compareTo(this.word)
    }
  }
}
object SecondarySortDemo {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setAppName("SecondarySortDemo").setMaster("local[*]")
    val sc = new SparkContext(sparkConf)
    val rdd: RDD[String] = sc.textFile("H:\\input1")
    val filterRdd: RDD[String] = rdd.filter(f => if (f.startsWith("(") && f.endsWith(")")) true else false)
    
    //(hainiu,100) ---> key,value
    val pairRdd: RDD[(SecondaryKey, String)] = filterRdd.map(f => {
      val str: String = f.substring(1, f.length - 1)
      val arr: Array[String] = str.split(",")
      val word: String = arr(0)
      val num: Int = arr(1).toInt
      //key实现了二次排序
      (new SecondaryKey(word, num), s"${word}\t${num}")
    })
    val sortRdd: RDD[(SecondaryKey, String)] = pairRdd.sortByKey()
    val arr: Array[(SecondaryKey, String)] = sortRdd.collect()
    for (t <- arr){
      println(t._2)
    }
    
  }
}

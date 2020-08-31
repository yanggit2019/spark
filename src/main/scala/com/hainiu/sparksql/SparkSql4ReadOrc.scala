package com.hainiu.sparksql

import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

object SparkSql4ReadOrc {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("SparkSql4ReadOrc").setMaster("local[*]")
    val sc = new SparkContext(conf)
    
    //根据SparkContext创建SQLContext
    val sqlc = new SQLContext(sc)
    //读取orc文件产生DataFrame
    val df: DataFrame = sqlc.read.orc("H:\\input_orc")
    df.printSchema()
    df.show()
  }
}

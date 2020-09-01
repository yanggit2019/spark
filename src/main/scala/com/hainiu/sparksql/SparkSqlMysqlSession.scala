package com.hainiu.sparksql

import org.apache.spark.sql.SparkSession

object SparkSqlMysqlSession {
  def main(args: Array[String]): Unit = {
//    val conf = new SparkConf()
//    conf.setAppName("SparkSqlMysqlSession")
//    conf.setMaster("local[*]")
//    conf.set("spark.sql.shuffle.partitions","1")
//
//    val session: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    val session: SparkSession = SparkSession.builder()
      .config("spark.sql.shuffle.partitions", "1")
      .config("spark.master", "local[*]")
      .appName("SparkSqlMysqlSession").getOrCreate()
    
    
    
    
    
    
    
    
    
    
    
  }
  
}

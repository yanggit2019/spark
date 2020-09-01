package com.hainiu.sparksql

import com.mysql.jdbc.Driver
import org.apache.spark.sql.{DataFrame, SparkSession}

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

    val data: DataFrame = session.read.format("jdbc")
      .option("driver", classOf[Driver].getName)
      .option("url", "jdbc:mysql://nn2.hadoop:3306/hainiucralwer")
      .option("dbtable", "hainiu_web_seed")
      .option("user", "hainiu")
      .option("password", "12345678").load()
    
    data.createOrReplaceTempView("tmp")

    val row: DataFrame = session.sql("select host from tmp")
    
    row.show()
    
    
  }
  
}

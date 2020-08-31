package com.hainiu.sparksql

import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

object SparkSql1Json {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("SparkSql1Json").setMaster("local[*]")
    val sc = new SparkContext(conf)
    
    //根据SparkContext创建SQLContext
    val sqlc = new SQLContext(sc)
    //读取json文件，生成dataframe
    val df: DataFrame = sqlc.read.json("H:\\input_json")
    df.printSchema()
    df.show()
  }
}

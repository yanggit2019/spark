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
    
    //查看country列中的内容
    df.select(df.apply("country")).show()
    df.select(df("country")).show()
    df.select("country","num").show()
    //查询所有country和num,并把num+1
    df.select(df("country"),(df("num")+1).as("num1")).show()
    //查询num<2的数据
    df.filter(df("num")<2).show()
  }
}

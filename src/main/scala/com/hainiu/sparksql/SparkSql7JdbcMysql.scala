package com.hainiu.sparksql

import java.util.Properties

import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

object SparkSql7JdbcMysql {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("SparkSql7JdbcMysql").setMaster("local[*]")
    val sc = new SparkContext(conf)
    
    //根据SparkContext创建SQLContext
    val sqlc = new SQLContext(sc)
    val prop = new Properties()
    prop.put("user","root")
    prop.put("password","123456")
    //通过jdbc连接MySQL
    val s1df: DataFrame = sqlc.read.jdbc("jdbc:mysql://localhost:3306/hainiu_test", "student", prop)
    val s2df: DataFrame = sqlc.read.jdbc("jdbc:mysql://localhost:3306/hainiu_test", "student_course", prop)
    s1df.createOrReplaceTempView("s")
    s2df.createOrReplaceTempView("sc")

    val joinDF: DataFrame = sqlc.sql("select * from s,sc where s.S_ID = sc.SC_S_ID")
    joinDF.printSchema()
    joinDF.show()
  }
}

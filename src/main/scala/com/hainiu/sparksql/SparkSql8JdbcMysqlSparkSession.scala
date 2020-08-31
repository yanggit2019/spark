package com.hainiu.sparksql

import java.util.Properties

import com.mysql.jdbc.Driver
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

object SparkSql8JdbcMysqlSparkSession {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("SparkSql8JdbcMysqlSparkSession").setMaster("local[*]")
    val sc = new SparkContext(conf)
    
    //根据SparkContext创建SparkSession对象
    val sparkSession = SparkSession.builder().config(conf).getOrCreate()
    val prop = new Properties()
    prop.put("user","root")
    prop.put("password","123456")
    //通过jdbc连接MySQL
    val s1df: DataFrame = sparkSession.read.jdbc("jdbc:mysql://localhost:3306/hainiu_test", "student", prop)
//    val s2df: DataFrame = sparkSession.read.jdbc("jdbc:mysql://localhost:3306/hainiu_test", "student_course", prop)
    //如果想增加个性化option，可以用下面方式
    val s2df: DataFrame = sparkSession.read.format("jdbc")
        .option("driver", classOf[Driver].getName)
        .option("url", "jdbc:mysql://localhost:3306/hainiu_test")
        .option("dbtable", "student")
        .option("user", "root")
        .option("password", "123456").load()
    s1df.createOrReplaceTempView("s")
    s2df.createOrReplaceTempView("sc")

    val joinDF: DataFrame = sparkSession.sql("select * from s,sc where s.S_ID = sc.SC_S_ID")
    joinDF.printSchema()
    joinDF.show()
  }
}

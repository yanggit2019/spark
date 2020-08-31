package com.hainiu.sparksql

import com.hainiu.util.MyPredef.string2HDFSUtil
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

object SparkSql7JdbcMysql {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("SparkSql7JdbcMysql").setMaster("local[*]")
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
    //按照country统计相同country的数量
    //select country,count(*) from XXX group by country
    val groupByDF: DataFrame = df.groupBy("country").count()
    groupByDF.printSchema()
    groupByDF.show()
    //将统计后的结果保存到hdfs上
    //DataFrame可以直接转rdd，而且转完是rdd[Row]
    val rdd: RDD[Row] = groupByDF.rdd
    //rdd[Row] --> rdd[String] String 里是“CN 2”
    val rdd2: RDD[String] = rdd.map(row => {
      //提取row里面的数据方法
      s"${row.getString(0)}\t${row.getLong(1)}"
    })
    //把200个分区降成一个
    val rdd3: RDD[String] = rdd2.coalesce(1)
    val outputDir:String = "/tmp/sparksql/output_json"
    outputDir.deleteHdfs()
    rdd3.saveAsTextFile(outputDir)
  }
}

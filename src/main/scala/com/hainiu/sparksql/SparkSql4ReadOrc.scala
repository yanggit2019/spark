package com.hainiu.sparksql

import org.apache.spark.sql.{DataFrame, Dataset, Row, SQLContext, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}

object SparkSql4ReadOrc {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("SparkSql4ReadOrc").setMaster("local[*]")
    
    //设置默认的shuffle产生的分区数
    conf.set("spark.sql.shuffle,partitions","1")
    val sc = new SparkContext(conf)
    
    //根据SparkContext创建SQLContext
    val sqlc = new SQLContext(sc)
    //读取orc文件产生DataFrame
    val df: DataFrame = sqlc.read.orc("H:\\input_orc")
    df.printSchema()
    df.show()
    //select country,num from
    //(select country,count(*) as num from XXXX group by country) t
    //where t.num > 5
    val groupByDF: DataFrame = df.groupBy("country").count()
    val filterDF: Dataset[Row] = groupByDF.filter(groupByDF("count") > 5)
//    filterDF.printSchema()
//    filterDF.show()
    //默认的缓存级别是MEMORY_AND_DISK
    val cacheDF: filterDF.type = filterDF.cache()
    //写入orc文件
    cacheDF.write.mode(SaveMode.Overwrite).format("orc").save("/tmp/sparksql/output_orc")
    //写入json文件
    cacheDF.write.mode(SaveMode.Overwrite).format("json").save("/tmp/sparksql/output_json")
  }
}

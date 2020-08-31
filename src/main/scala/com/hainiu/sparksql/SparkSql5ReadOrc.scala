package com.hainiu.sparksql

import org.apache.spark.sql._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

object SparkSql5ReadOrc {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("SparkSql5ReadOrc").setMaster("local[*]")
    
    //设置默认的shuffle产生的分区数
    conf.set("spark.sql.shuffle,partitions","1")
    val sc = new SparkContext(conf)
    
    //根据SparkContext创建SQLContext
    val hqlc = new HiveContext(sc)
    //读取orc文件产生DataFrame
    val df: DataFrame = hqlc.read.orc("H:\\input_orc")
//    df.printSchema()
//    df.show()
    //select country,num from
    //(select country,count(*) as num from XXXX group by country) t
    //where t.num > 5
    //创建临时视图
    df.createOrReplaceTempView("df_table")
    val sql =
      """
        |select country,num from
        |(select country,count(*) as num from df_table group by country) t
        |where t.num > 5
        |""".stripMargin
    val redDs: DataFrame = hqlc.sql(sql)
    redDs.printSchema()
    redDs.show()
    //覆盖以文本形式写入文件
    redDs.write.mode(SaveMode.Overwrite).format("text").save("/tmp/sparksql/output_text5")
    
  }
}

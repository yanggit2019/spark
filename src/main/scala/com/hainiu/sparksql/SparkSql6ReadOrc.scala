package com.hainiu.sparksql

import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

object SparkSql6ReadOrc {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("SparkSql6ReadOrc").setMaster("local[*]")
    
    //设置默认的shuffle产生的分区数
    conf.set("spark.sql.shuffle,partitions","1")
    val sc = new SparkContext(conf)
    
    //根据SparkContext创建SQLContext
    val hqlc = new HiveContext(sc)
    
    //建库
    hqlc.sql("create database if not exists class23")
    //进入class23
    hqlc.sql("use class23")
    //建表
    hqlc.sql(
      """
        |CREATE TABLE if not exists `user_orc`(
        |  `aid` string COMMENT 'from deserializer',
        |  `pkgname` string COMMENT 'from deserializer',
        |  `uptime` bigint COMMENT 'from deserializer',
        |  `type` int COMMENT 'from deserializer',
        |  `country` string COMMENT 'from deserializer',
        |  `gpcategory` string COMMENT 'from deserializer')
        |ROW FORMAT SERDE
        |  'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
        |STORED AS INPUTFORMAT
        |  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
        |OUTPUTFORMAT
        |  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
      """.stripMargin)
    //导入数据
    hqlc.sql("load data local inpath '/input_orc' overwrite into table user_orc")
    //读取orc文件产生DataFrame
//    val df: DataFrame = hqlc.read.orc("H:\\input_orc")
////    df.printSchema()
////    df.show()
//    //select country,num from
//    //(select country,count(*) as num from XXXX group by country) t
//    //where t.num > 5
    //创建临时视图
//    df.createOrReplaceTempView("df_table")
    val sql =
      """
        |select concat(country,num) as country_num from
        |(select country,count(*) as num from user_orc group by country) t
        |where t.num > 5
        |""".stripMargin
    val redDs: DataFrame = hqlc.sql(sql)
    redDs.printSchema()
    redDs.show()
    //覆盖以文本形式写入文件
    redDs.write.mode(SaveMode.Overwrite).format("text").save("/tmp/sparksql/output_text5")
    
  }
}

package com.hainiu.sparksql

import com.mysql.jdbc.Driver
import org.apache.spark.sql.{DataFrame, SparkSession}

object SparkSqlMysqlSessionUDF {
  def main(args: Array[String]): Unit = {
    val session: SparkSession = SparkSession.builder()
      .config("spark.sql.shuffle.partitions", "1")
      .config("spark.master", "local[*]")
      .appName("SparkSqlMysqlSession").getOrCreate()

    import session.implicits._

    val data: DataFrame = session.read.format("jdbc")
      .option("driver", classOf[Driver].getName)
      .option("url", "jdbc:mysql://nn2.hadoop:3306/hainiucralwer")
      .option("dbtable", "hainiu_web_seed")
      .option("user", "hainiu")
      .option("password", "12345678").load()
    
    data.createOrReplaceTempView("temp")
    
    session.udf.register("hainiu",(host:String) =>{
      s"bigNiu_${host}"
    })

    val frame: DataFrame = session.sql("select hainiu(host) from temp")
    
    frame.show()
  }
}

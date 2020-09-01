package com.hainiu.sparksql

import com.mysql.jdbc.Driver
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object SparkSqlMysqlSessionDataSet {
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
    val mysqlRow: DataFrame = session.sql("select host from temp")
    
//    mysqlRow.map( r =>{
//      val host: String = r.getString(0)
//      host
//    })
    import session.implicits._
    val ds: Dataset[HainiuSparkDataSet] = mysqlRow map {
      case host: Row => HainiuSparkDataSet(s"hainiu_${host.mkString}")
      case _ => HainiuSparkDataSet("None")
    }
    ds.show()
    //转成dataframe
//    ds.toDF()
    //转成rdd
//    ds.rdd
  }
}

case class HainiuSparkDataSet(val host:String)
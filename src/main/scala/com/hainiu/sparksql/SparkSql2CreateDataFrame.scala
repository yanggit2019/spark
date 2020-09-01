package com.hainiu.sparksql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

object SparkSql2CreateDataFrame {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("SparkSql2CreateDataFrame").setMaster("local[*]")
    val sc = new SparkContext(conf)

    //根据SparkContext创建SQLContext
    val sqlc = new SQLContext(sc)

    val rdd: RDD[String] = sc.textFile("H:\\input_text")
    val rddRow: RDD[Row] = rdd.map(f => {
      val arr: Array[String] = f.split("\t")
      val country: String = arr(0)
      val gpcategory: String = arr(1)
      val pkgname: String = arr(2)
      val num: Int = arr(3).toInt
      Row(country, gpcategory, pkgname, num)
    })
    val fields = new ArrayBuffer[StructField]
    fields += new StructField("country",DataTypes.StringType,true)
    fields += new StructField("gpcategory",DataTypes.StringType,true)
    fields += new StructField("pkgname",DataTypes.StringType,true)
    fields += new StructField("num",DataTypes.IntegerType,true)
    val structType: StructType = StructType(fields)
    //通过Row里面的数据，来映射出字段，构建DataFrame
    val df: DataFrame = sqlc.createDataFrame(rddRow, structType)
    df.printSchema()
    df.show()
    //select country，count(*) as count_num from XXX group by country
    
    val groupByDF: DataFrame = df.groupBy("country").count()
    groupByDF.show()
  }
}

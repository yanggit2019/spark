package com.hainiu.sparksql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer
class DFBean(val country: String,val gpcategory:String,val pkgname:String,val num:Int){
  //定义getXXX的方法，比如getCountry的方法，框架会通过反射的方式，用属性拼接方法，调用该方法
  def getCountry = this.country
  def getGpcategory=this.gpcategory
  def getPkgname = this.pkgname
  def getNum = this.num
  
}
object SparkSql3CreateDataFrameByBean {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("SparkSql3CreateDataFrame").setMaster("local[*]")
    val sc = new SparkContext(conf)

    //根据SparkContext创建SQLContext
    val sqlc = new SQLContext(sc)

    val rdd: RDD[String] = sc.textFile("H:\\input_text")
    val rddBean: RDD[DFBean] = rdd.map(f => {
      val arr: Array[String] = f.split("\t")
      val country: String = arr(0)
      val gpcategory: String = arr(1)
      val pkgname: String = arr(2)
      val num: Int = arr(3).toInt
      new DFBean(country, gpcategory, pkgname, num)
    })
    val fields = new ArrayBuffer[StructField]
    fields += new StructField("country",DataTypes.StringType,true)
    fields += new StructField("gpcategory",DataTypes.StringType,true)
    fields += new StructField("pkgname",DataTypes.StringType,true)
    fields += new StructField("num",DataTypes.IntegerType,true)
    val structType: StructType = StructType(fields)
    //通过Row里面的数据，来映射出字段，构建DataFrame
    val df: DataFrame = sqlc.createDataFrame(rddBean, classOf[DFBean])
    df.printSchema()
    df.show()
    //通过把DataFrame的数据集创建临时视图来通过SQL查询方式来执行程序
    //select country，count(*) as count_num from XXX group by country
    //通过Beanclass来构建DataFrame,这个beanclass必须定义属性的getXXX方法
    df.createOrReplaceTempView("df_table")
    //根据临时视图名称来写SQL执行
    val groupByDf: DataFrame = sqlc.sql("select country,count(*) as count_num from df_table group by country")
    groupByDf.printSchema()
    groupByDf.show()
  }
}

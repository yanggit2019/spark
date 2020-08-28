package com.hainiu.hbase

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, HTable, Put}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


object SparkHbase2Puts {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkHbase2Puts")
    val sc = new SparkContext(sparkConf)
    val rdd: RDD[Int] = sc.parallelize(20 until 30, 2)
    //一个分区创建一个连接，批量写入
    rdd.foreachPartition(it =>{
      //把迭代器里的每条数据，转换成put对象，并且返回新的迭代器，用于写hbase
      val puts: Iterator[Put] = it.map(f => {
        
        val put = new Put(Bytes.toBytes(s"spark_put_${f}"))
        put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("count"), Bytes.toBytes(s"${f}"))
        put
      })
      val hbaseConf: Configuration = HBaseConfiguration.create()
      var conn:Connection = null
      var  hTable:HTable =null
      //一个分区创建连接，批量写
      try{
        //创建hbase 连接
        conn = ConnectionFactory.createConnection(hbaseConf)
        //获取表操作对象
        hTable = conn.getTable(TableName.valueOf("lyy23:spark_user")).asInstanceOf[HTable]
        //通过隐式转换实现给scala的List赋予javaList的功能
        import scala.collection.convert.wrapAsJava.seqAsJavaList
        //写入一个分区数据
        hTable.put(puts.toList)
      }catch {
        case e:Exception => e.printStackTrace()
      }finally {
        hTable.close()
        conn.close()
      }
      
    })
  } 
}

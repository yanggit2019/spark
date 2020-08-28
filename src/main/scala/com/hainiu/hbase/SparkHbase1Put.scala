package com.hainiu.hbase

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, HTable, Put}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


object SparkHbase1Put {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkHbase1Put")
    val sc = new SparkContext(sparkConf)
    val rdd: RDD[Int] = sc.parallelize(10 until 20, 2)
    rdd.foreach( f =>{
      //获取hbase配置对象
      val hbaseConf: Configuration = HBaseConfiguration.create()
      var conn:Connection = null
      var  hTable:HTable =null
      try{
        
      //创建hbase 连接
       conn = ConnectionFactory.createConnection(hbaseConf)
      //获取表操作对象
       hTable = conn.getTable(TableName.valueOf("lyy23:spark_user")).asInstanceOf[HTable]

      val put = new Put(Bytes.toBytes(s"spark_put_${f}"))
      put.addColumn(Bytes.toBytes("cf"),Bytes.toBytes("count"),Bytes.toBytes(s"${f}"))
      //写入数据
      hTable.put(put)
      }catch {
        case e:Exception => e.printStackTrace()
      }finally {
      hTable.close()
      conn.close()
      }
    }
    )
  }
}

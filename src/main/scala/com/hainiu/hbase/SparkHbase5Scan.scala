package com.hainiu.hbase

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Result, Scan}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{TableInputFormat, TableMapReduceUtil}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


object SparkHbase5Scan {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkHbase5Scan")
    val sc = new SparkContext(sparkConf)
    val scan = new Scan()
    val hbaseConf: Configuration = HBaseConfiguration.create()
    //读取hbase表所用的表名
    hbaseConf.set(TableInputFormat.INPUT_TABLE,"lyy23:spark_user")
    //读取hbase表所用的scan范围
    hbaseConf.set(TableInputFormat.SCAN,TableMapReduceUtil.convertScanToString(scan))
//    conf: Configuration = hadoopConfiguration,
//    fClass: Class[F],
//    kClass: Class[K],
//    vClass: Class[V]
    val rdd: RDD[(ImmutableBytesWritable, Result)] = sc.newAPIHadoopRDD(hbaseConf,
    classOf[TableInputFormat],
    classOf[ImmutableBytesWritable],
    classOf[Result]
    )
    val resRdd: RDD[String] = rdd.map(
      f => {
        //ImmutableBytesWritable 里面有byte[]代表rowkey
        val rowkey: String = Bytes.toString(f._1.get())
        val result: Result = f._2
        val value: String = Bytes.toString(result.getValue(Bytes.toBytes("cf"), Bytes.toBytes("count")))
        (s"${rowkey} column=cf:count,value=${value}")
      }
    )
    //生成的rdd分区数，取决去读取hbase表的region数
    println(resRdd.getNumPartitions)
    val arr: Array[String] = resRdd.collect()
    for (s <- arr){
      println(s)
    }
    }
    
  
}

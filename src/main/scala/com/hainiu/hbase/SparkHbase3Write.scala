package com.hainiu.hbase

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SparkHbase3Write {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkHbase3Write")
    val sc = new SparkContext(sparkConf)
    val rdd: RDD[Int] = sc.parallelize(20 until 40, 2)
    val writeHbaseRdd: RDD[(NullWritable, Put)] = rdd.map(f => {
      val put = new Put(Bytes.toBytes(s"spark_write_${f}"))
      put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("count"), Bytes.toBytes(s"${f}"))
      (NullWritable.get(), put)
    }
    )
    val hbaseConf: Configuration = HBaseConfiguration.create()
    //添加表名
    hbaseConf.set(TableOutputFormat.OUTPUT_TABLE,"lyy23:spark_user")
    val job: Job = Job.getInstance(hbaseConf)
    //设置输出formatclass
    job.setOutputFormatClass(classOf[TableOutputFormat[NullWritable]])
    //设置输出keyclass
    job.setOutputKeyClass(classOf[NullWritable])
    //设置输出的valueclass
    job.setOutputValueClass(classOf[Put])
    //一个分区创建一个连接，一条一条写入
    writeHbaseRdd.saveAsNewAPIHadoopDataset(job.getConfiguration)
  }
  
}

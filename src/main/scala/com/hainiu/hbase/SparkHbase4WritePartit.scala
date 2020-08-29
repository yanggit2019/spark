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

object SparkHbase4WritePartit {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkHbase4WritePartition")
    val sc = new SparkContext(sparkConf)
    val rdd: RDD[Int] = sc.parallelize(50 until 99, 20)
    val writeHbaseRdd: RDD[(NullWritable, Put)] = rdd.map(f => {
      val put = new Put(Bytes.toBytes(s"spark_write_p_${f}"))
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
    
    println(s"修改分区前分区数:${writeHbaseRdd.getNumPartitions}")
    //后面Spark SQL的groupby操作会产生200个分区，可以通过coalesce减少分区数
    val coalesceRdd: RDD[(NullWritable, Put)] = writeHbaseRdd.coalesce(2)
    println(s"修改分区后分区数:${coalesceRdd.getNumPartitions}")
    //一个分区创建一个连接，一条一条写入
    writeHbaseRdd.saveAsNewAPIHadoopDataset(job.getConfiguration)
  }
  
}

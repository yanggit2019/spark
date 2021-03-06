package com.hainiu.hbase

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, HTable}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{HFileOutputFormat2, LoadIncrementalHFiles}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, KeyValue, TableName}
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer
class SparkHbase7Hfile
object SparkHbase7Hfile {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf()
//      .setMaster("local[*]") //注释掉
      .setAppName("SparkHbase7Hfile")
    //开启Kryo序列化
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(sparkConf)
//    val hbaseTableNam:String = "lyy23:spark_load"
    //通过传参来赋值
    val hbaseTableNam:String = args(0)
    val outputDir:String = args(1)
     
    val rdd: RDD[Int] = sc.parallelize(10 until 20, 2)
    //rdd[Int]--->rdd【(key(可以实现二次比较),keyValue)】
    val pairRdd: RDD[(HbaseSecondaryKey, KeyValue)] = rdd.mapPartitions(it => {
      val list = new ListBuffer[(HbaseSecondaryKey, KeyValue)]

      it.foreach(f => {
        val rowkey = new ImmutableBytesWritable(Bytes.toBytes(s"spark_hfile_${f}"))
        val keyValue1 = new KeyValue(rowkey.get(), Bytes.toBytes("cf"), Bytes.toBytes("count"), Bytes.toBytes(s"${f}"))
        val keyValue2 = new KeyValue(rowkey.get(), Bytes.toBytes("cf"), Bytes.toBytes("name"), Bytes.toBytes(s"name_${f}"))
        list += ((new HbaseSecondaryKey(rowkey, keyValue1), keyValue1))
        list += ((new HbaseSecondaryKey(rowkey, keyValue2), keyValue2))
      })
      list.iterator
    })
    //实现按照key进行二次排序
    val sortRdd: RDD[(HbaseSecondaryKey, KeyValue)] = pairRdd.sortByKey()
    //获取能写入hfile文件的rdd
    val writeHfileRdd: RDD[(ImmutableBytesWritable, KeyValue)] = sortRdd.map(f => (f._1.rowkey, f._2))
    val hbaseConf: Configuration = HBaseConfiguration.create()
    val conn: Connection = ConnectionFactory.createConnection(hbaseConf)
    val table: HTable = conn.getTable(TableName.valueOf(hbaseTableNam)).asInstanceOf[HTable]
    val job: Job = Job.getInstance(hbaseConf)
    //加载能写入hfile文件的配置
    HFileOutputFormat2.configureIncrementalLoad(job,table.getTableDescriptor,table.getRegionLocator)
    
//    val outputDir:String ="/tmp/spark/hbase"
    import com.hainiu.util.MyPredef.string2HDFSUtil
    outputDir.deleteHdfs()
    writeHfileRdd.saveAsNewAPIHadoopFile(outputDir,
      classOf[ImmutableBytesWritable],
      classOf[KeyValue],
      classOf[HFileOutputFormat2],
      job.getConfiguration
    )
    //本地无法运行，需要打包上集群
    //将生成的hfile文件导入到hbase表中，参考下面的导入命令
    //hadoop jar /usr/local/hbase/lib/hbase-shell-1.3.1.jar completebulkload /user/panniu/spark/hbase_bulk_output panniu:spark_load
    val arr = Array[String](outputDir,hbaseTableNam)
    
    LoadIncrementalHFiles.main(arr)
  }
}

class HbaseSecondaryKey2(val rowkey:ImmutableBytesWritable,val keyValue:KeyValue) extends Ordered[HbaseSecondaryKey2]{
  override def compare(that: HbaseSecondaryKey2): Int = {
    if (this.rowkey.compareTo(that.rowkey)==0){
      //利用KeyValue的外部比较器实现KeyValue的比较
      KeyValue.COMPARATOR.compare(this.keyValue,that.keyValue)
    }else{
      this.rowkey.compareTo(that.rowkey)
    }
  }
} 
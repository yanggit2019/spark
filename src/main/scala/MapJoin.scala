import com.hainiu.util.{OrcFormat, OrcUtil}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hive.ql.io.orc.{OrcNewInputFormat, OrcNewOutputFormat, OrcStruct}
import org.apache.hadoop.io.{NullWritable, Writable}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkConf, SparkContext}
import org.xerial.snappy.SnappyCodec

import scala.collection.mutable.ListBuffer
import scala.io.Source

object MapJoin {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("mapjoin")
    val sc = new SparkContext(sparkConf)
//    path: String,
//    fClass: Class[F],
//    kClass: Class[K],
//    vClass: Class[V],
//    conf: Configuration
    val orcPath = "H:\\input2"
    val orcRdd: RDD[(NullWritable, OrcStruct)] = sc.newAPIHadoopFile(orcPath,
      classOf[OrcNewInputFormat],
      classOf[NullWritable],
      classOf[OrcStruct]
    )
    //加载字典文件到广播变量
    val lines: List[String] = Source.fromFile("/tmp/spark/country_dict.dat").getLines().toList
    val dictMap: Map[String, String] = lines.map(_.split("\t")).map(f => (f(0), f(1))).toMap
    val broad: Broadcast[Map[String, String]] = sc.broadcast(dictMap)
    
    //定义能匹配上的累加器
    val matchAcc: LongAccumulator = sc.longAccumulator
    //定义不能匹配上的累加器
    val notMatchAcc: LongAccumulator = sc.longAccumulator
    val writeOrcRdd: RDD[(NullWritable, Writable)] = orcRdd.mapPartitionsWithIndex((index, it) => {
      //分区内创建OrcUtil对象，设置读inspector，设置写inspector
      val orcUtil = new OrcUtil
      orcUtil.setOrcTypeReadSchema(OrcFormat.SCHEMA)
      orcUtil.setOrcTypeWriteSchema("struct<code:string,name:string>")
      //从广播变量里面取出字典Map
      val dictMap2: Map[String, String] = broad.value
      //定义能装写入orc格式的对象
      val list = new ListBuffer[(NullWritable, Writable)]

      it.foreach(f => {
        val countryCode: String = orcUtil.getOrcData(f._2, "country")
        val option: Option[String] = dictMap2.get(countryCode)
        if (option == None) {
          notMatchAcc.add(1L)
        } else {
          matchAcc.add(1L)
          val countryName: String = option.get
          orcUtil.addAttr(countryCode, countryName)
          val w: Writable = orcUtil.serialize()

          list += ((NullWritable.get(), w))
        }
      })
      list.iterator
    })
    val outputDir:String ="/tmp/spark/mapjoin_output"
    import com.hainiu.util.MyPredef.string2HDFSUtil
    outputDir.deleteHdfs()
    val hadoopConf = new Configuration()
    hadoopConf.set("orc.compress",classOf[SnappyCodec].getName);
//    path: String,
//    keyClass: Class[_],
//    valueClass: Class[_],
//    outputFormatClass: Class[_ <: NewOutputFormat[_, _]],
//    conf: Configuration
    writeOrcRdd.saveAsNewAPIHadoopFile(outputDir,
      classOf[NullWritable],
      classOf[Writable],
      classOf[OrcNewOutputFormat],
      hadoopConf
    )
    println(s"matchAcc,${matchAcc}")
    println(s"notmatchAcc,${notMatchAcc}")
  }
}

package com.hainiu.sparkcore

import com.esotericsoftware.kryo.Kryo
import org.apache.hadoop.io.Text
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.KryoRegistrator
import org.apache.spark.{SparkConf, SparkContext}

object SerDemo {
  def main(args: Array[String]): Unit = {
    val SparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SerDemo")
    //开启Kryo序列化
    SparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    //要求主动注册
    SparkConf.set("spark.kryo.registrationRequired","true")
   //第一种方式注册
//   val classes: Array[Class[_]] = Array[Class[_]](classOf[UserInfo]
//      ,classOf[Text]
//      ,Class.forName("scala.collection.mutable.WrappedArray$ofRef")
//      ,classOf[Array[Int]],classOf[Array[String]]
//      ,Class.forName("scala.reflect.ClassTag$$anon$1")
//     ,Class.forName("java.lang.Class")
//     ,classOf[Array[UserInfo]]
//   )
    // 将上面的类注册
//    SparkConf.registerKryoClasses(classes)
    SparkConf.set("spark.kryo.registrator",classOf[MyRegistrator].getName)
    
    val sc = new SparkContext(SparkConf)
    val rdd: RDD[String] = sc.parallelize(List("aa", "aa", "bb", "aa"), 2)
    val broad: Broadcast[UserInfo] = sc.broadcast(new UserInfo)

    val pairRdd: RDD[(String, UserInfo)] = rdd.map(f => {
      val pairRdd: UserInfo = broad.value
      (f, pairRdd)
    })
    //因为groupByKey有shuffle,需要序列化
    val groupRdd: RDD[(String, Iterable[UserInfo])] = pairRdd.groupByKey()
    val arr: Array[(String, Iterable[UserInfo])] = groupRdd.collect()
    for (t <- arr){
      println(t)
    }
  }
}
class MyRegistrator extends KryoRegistrator{
  override def registerClasses(kryo: Kryo): Unit = {
    kryo.register(classOf[UserInfo])
    kryo.register(classOf[Text])
    kryo.register(Class.forName("scala.collection.mutable.WrappedArray$ofRef"))
    kryo.register(classOf[Array[UserInfo]])
    kryo.register(classOf[Array[String]])
    kryo.register(Class.forName("java.lang.Class"))
    kryo.register(Class.forName("scala.reflect.ClassTag$$anon$1"))
  }
}
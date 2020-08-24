package com.hainiu.util

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

class HDFSUtil(val path:String) {
  def deleteHdfs() ={
    val hadoopConf = new Configuration
        val fs:FileSystem = FileSystem.get(hadoopConf)
        val outputPath = new Path(path)
        if (fs.exists(outputPath)){
          fs.delete(outputPath,true)
          println(s"delete outputpath:[${path} success!]")
        }
  }
}

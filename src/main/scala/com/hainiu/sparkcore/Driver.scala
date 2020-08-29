package com.hainiu.sparkcore

import com.hainiu.hbase.SparkHbase7Hfile
import org.apache.hadoop.util.ProgramDriver

object Driver {
  def main(args: Array[String]): Unit = {
    val driver = new ProgramDriver
    driver.addClass("mapjoin",classOf[MapJoin],"通过国家码和国家名称join")
    driver.addClass("spark_load",classOf[SparkHbase7Hfile],"写入hfile文件，并导入hbase表")
    driver.run(args)
  }
}

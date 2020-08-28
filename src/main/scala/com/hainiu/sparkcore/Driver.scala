package com.hainiu.sparkcore

import org.apache.hadoop.util.ProgramDriver

object Driver {
  def main(args: Array[String]): Unit = {
    val driver = new ProgramDriver
    driver.addClass("mapjoin",classOf[MapJoin],"通过国家码和国家名称join")
    driver.run(args)
  }
}

package com.hainiu.util

object MyPredef {
  //定义隐式转换函数实现字符串转HDFSUtil对象
  implicit def string2HDFSUtil(path:String) = new HDFSUtil(path)
}

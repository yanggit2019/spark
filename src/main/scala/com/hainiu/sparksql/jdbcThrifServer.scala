package com.hainiu.sparksql



import java.sql.{Connection, DriverManager, ResultSet, Statement}

import org.apache.hive.jdbc.HiveDriver

object jdbcThrifServer {
  def main(args: Array[String]): Unit = {
    //加载Driver
    classOf[HiveDriver]
    var conn:Connection = null
    var stmt: Statement = null
    try{
      //创建connection
      conn = DriverManager.getConnection("jdbc:hive2://op.hadoop:20000/liuyiyang21", "", "")
      //创建statement
       stmt= conn.createStatement()
      //为了groupby少生成任务数，设置默认shuffle的分区
      stmt.execute("set spark.sql.shuffle.partitions=20")
      val sql:String =
        """
          |select sum(num) from 
          |(select count(*) as num from user_install_status group by country)a
          |""".stripMargin
      //执行SQL，得到查询结果
      val rs: ResultSet = stmt.executeQuery(sql)
      //获取结果
      while (rs.next()){
        val num: Long = rs.getLong(1)
        println(s"jdbc请求thriftserver后返回的查询结果:${num}")
      }
    }catch {
      case e:Exception => e.printStackTrace()
    }finally {
      stmt.close()
      conn.close()
    }

     
    
  }
}

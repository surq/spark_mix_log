package com.asiainfo.mysql.test

import com.asiainfo.mix.streaming_log.LogTools
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.Map
import com.asiainfo.mix.spark.XmlProperiesAnalysis
import java.util.Calendar

object MysqlTest {

  def main(args: Array[String]): Unit = {

    val dbSourceMap = Map[String, String]()
    dbSourceMap += ("driver" -> "com.mysql.jdbc.Driver")
    dbSourceMap += ("url" -> "jdbc:mysql://rdsz3yvjiz3yvji.mysql.rds.aliyuncs.com:3306/dmp_console_test")
    dbSourceMap += ("user" -> "dmp_console_test")
    dbSourceMap += ("password" -> "a25a_Uagq")

    val bean = new dbBean()

    val nowTime = Calendar.getInstance().getTimeInMillis()
    var count = 0
    var secode_flg = true
    var secode_ten_flg = true
    var loop_flg = true
    val connection = LogTools.getConnection(dbSourceMap.toArray)
    while (loop_flg) {
      if (secode_flg && Calendar.getInstance().getTimeInMillis() > nowTime + 1000) {
        secode_flg = false
        println("per seconde:" + count)
      } else if (secode_ten_flg && Calendar.getInstance().getTimeInMillis() > nowTime + 60000) {
        secode_ten_flg = false
        println("per one minute:" + count)
        loop_flg = false
      } else if (secode_ten_flg && Calendar.getInstance().getTimeInMillis() > nowTime + 600000) {
        secode_ten_flg = false
        println("per ten minute:" + count)
      }

      insert(connection, bean)
      count += 1
    }
    LogTools.closeConnection(connection)
  }

  /**
   * insert
   */
  //  def insert(dbSourceArray: Map[String,String], bean: dbBean) = {
  def insert(connection: java.sql.Connection, bean: dbBean) = {

    connection.prepareStatement("select activity_id from dsp_report_data where activity_id=10").executeQuery()

    val items = bean.toString
    var mark = ""
    0 until items.split(",").size foreach (p => {
      mark = mark + "?,"
    })

    val sql = "insert into dsp_report_data(" + items + ") values(" + mark.substring(0, mark.length - 1) + ")"
    val insertStatement = connection.prepareStatement(sql)
    insertStatement.setInt(1, bean.getActivity_id)
    insertStatement.setInt(2, bean.getOrder_id)
    insertStatement.setInt(3, bean.getMaterial_id)
    insertStatement.setInt(4, bean.getAd_id)
    insertStatement.setInt(5, bean.getSize_id)
    insertStatement.setInt(6, bean.getArea_id)
    insertStatement.setString(7, bean.getStart_time)
    insertStatement.setString(8, bean.getEnd_time)
    insertStatement.setString(9, bean.getMedia)
    insertStatement.setString(10, bean.getAd_pos_id)
    insertStatement.setInt(11, bean.getBid_cnt)
    insertStatement.setInt(12, bean.getBid_success_cnt)
    insertStatement.setInt(13, bean.getExpose_cnt)
    insertStatement.setInt(14, bean.getClick_cnt)
    insertStatement.setInt(15, bean.getArrive_cnt)
    insertStatement.setInt(16, bean.getSecond_jump_cnt)
    insertStatement.setInt(17, bean.getTrans_cnt)
    insertStatement.setInt(18, bean.getCost)
    insertStatement.setInt(1, bean.getActivity_id)
    insertStatement.executeUpdate()

  }

}
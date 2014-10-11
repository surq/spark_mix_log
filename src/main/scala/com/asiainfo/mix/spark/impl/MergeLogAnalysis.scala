package com.asiainfo.mix.spark.impl

import com.asiainfo.mix.streaming_log.StreamAction
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext._
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.dstream.DStream
import com.asiainfo.mix.streaming_log.LogTools
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.Logging
import com.asiainfo.spark.stream.report.LogReportUtil
import java.util.Calendar
import org.apache.spark.rdd.RDD
import com.asiainfo.mix.spark.XmlProperiesAnalysis
import com.asiainfo.mix.spark.BatchStream

/**
 * @author surq
 * @since 2014.09.25
 * @param rdds:各类型log处理完成后的rdd<br>
 * 汇总日志更新mysql 流处理<br>
 */
class MergeLogAnalysis(rdds: RDD[String]) extends Serializable {

  /**
   * union rdd后的rdd计划处理<br>
   */
  def merge: RDD[String] = {

    // properiesMap:applacation properies 配置<br>
    // HDFSfilePathMap:输入日志文件类型以及路径(HDFS)配置<br>
    // dbSourceMap:db(mysql) 驱动配置<br>
    // logStructMap：log 日志属性配置<br>
    // tablesDefMap：mysql表定义配置<br>
    val properties = XmlProperiesAnalysis.xmlProperiesAnalysis
    val properiesMap = properties._1
    val dbSourceArray = properties._3
    val logPropertiesMap = properties._4
    val tablesMap = properties._5
    // 各文件流联合成同一个RDD时，用的共通分隔符
    val unionseparator = properiesMap("unionseparator")
    // 日志记录在数据库中的时间间隔，时间间隔在logSpace内的将在db中产生一条，从业务上讲所有log类型的logSpace应该是一致的（单位：分钟）
    val logSpace = properiesMap("logSpace")
    // rowkey 连接符
    val separator = "asiainfoMixSeparator"

    //输出为表结构样式　用
    val tbname = tablesMap("tableName")
    val items = "rowKey," + tablesMap("items") + ",log_length"
    rdds.map(f => {
      (items.split(",")).zip(f.split(unionseparator))
    }).map(record => {
      val itemMap = record.toMap
      val rowKey = itemMap("rowKey")
      (rowKey, record)
    }).groupByKey.map(f => {

      // 创建db表结构并初始化
      var dbrecord = Map[String, String]()
      // 去除前取数据处理时，拼接的rowKey字段和长度，此字段数据库中不存在
      val exceptItems = Array("rowKey", "log_length")
      // 汇总所有类型log日志更新的字段
      f._2.foreach(record => {
        val items = for (enum <- record if (enum._2 != "")) yield enum
        items.foreach(f => {
          if (!exceptItems.contains(f._1)) {
            dbrecord += ((f._1) -> (dbrecord.getOrElse(f._1, "0").toFloat + f._2.toFloat).toString)
          }
        })
      })

      // 填充db表结构中的主key部分
      val keyarray = (f._1).split(separator)
      dbrecord += (("activity_id") -> keyarray(0))
      dbrecord += (("order_id") -> keyarray(1))
      dbrecord += (("material_id") -> keyarray(2))
      dbrecord += (("ad_id") -> keyarray(3))
      dbrecord += (("size_id") -> keyarray(4))
      dbrecord += (("area_id") -> keyarray(5))
      dbrecord += (("media") -> keyarray(6))
      dbrecord += (("ad_pos_id") -> keyarray(7))
      val logdate = getlogtime(keyarray(8), logSpace)
      dbrecord += (("start_time") -> logdate._1)
      dbrecord += (("end_time") -> logdate._2)

      val selectKeyArray = ArrayBuffer[(String, String)]()
      selectKeyArray += (("activity_id") -> keyarray(0))
      selectKeyArray += (("order_id") -> keyarray(1))
      selectKeyArray += (("material_id") -> keyarray(2))
      selectKeyArray += (("ad_id") -> keyarray(3))
      selectKeyArray += (("size_id") -> keyarray(4))
      selectKeyArray += (("area_id") -> keyarray(5))
      selectKeyArray += (("media") -> keyarray(6))
      selectKeyArray += (("ad_pos_id") -> keyarray(7))
      selectKeyArray += (("start_time") -> logdate._1)
      selectKeyArray += (("end_time") -> logdate._2)
      val tuple3: Tuple3[String, Map[String, String], ArrayBuffer[(String, String)]] = (f._1, dbrecord, selectKeyArray)
      tuple3
    }).mapPartitions(mp => {
      val starttime_sql = Calendar.getInstance().getTimeInMillis()
      val connection = LogTools.getConnection(dbSourceArray.toArray)
      var count = 0l
      val tmpList: ArrayBuffer[String] = ArrayBuffer[String]()
      for (patition <- mp) {
        tmpList += patition._1
        val dbrecord = patition._2
        val selectKeyArray = patition._3
        count = count + 1
        // 调用db查看有无数据存在
        //无则插入，有则更新
        LogTools.updataMysql(tbname, connection, selectKeyArray.toArray, dbrecord.toArray)
      }
      LogTools.closeConnection(connection)
      tmpList.iterator
    }).map(f => { f })
  }

  /**
   * 根据logtime的时间，取logtime所在的时间范围<br>
   */
  def getlogtime(logdate: String, logSpace: String): Tuple2[String, String] = {
    // 补全起止时间格式
    var start_time = ((logdate.substring(10)).toInt * logSpace.toInt).toString
    start_time = "00" + start_time
    start_time = start_time.substring(start_time.length - 2)
    start_time = logdate.substring(0, 10) + start_time + "00"
    var end_time = ((((logdate.substring(10)).toInt + 1) * logSpace.toInt) - 1).toString
    end_time = "00" + end_time
    end_time = end_time.substring(end_time.length - 2)
    end_time = logdate.substring(0, 10) + end_time + "59"
    (start_time, end_time)
  }
}
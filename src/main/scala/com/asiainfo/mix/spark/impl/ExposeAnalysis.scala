package com.asiainfo.mix.spark.impl

import com.asiainfo.mix.streaming_log.StreamAction
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext._
import com.asiainfo.mix.streaming_log.LogTools
import org.apache.spark.Logging
import scala.collection.mutable.ArrayBuffer
import com.asiainfo.spark.stream.report.LogReportUtil
import com.asiainfo.mix.spark.BatchStream
import org.apache.spark.rdd.RDD
import com.asiainfo.mix.spark.XmlProperiesAnalysis
import com.asiainfo.mix.spark.MixLog

/**
 * @author surq
 * @since 2014.09.25
 * 曝光日志 batch处理
 */
class ExposeAnalysis extends BatchStream with MixLog with Serializable {

  /**
   * @param topicLogType:日志类型<br>
   * @param inputRDD:数据源<br>
   * 输入的文件结构多了一个头：rowkey，多一个尾:size<br>
   */
  override def run(topicLogType:String, inputRDD: RDD[Array[(String, String)]]): RDD[String] = {
    printInfo(this.getClass(), "ExposeAnalysis is running!")

    // properiesMap:applacation properies 配置<br>
    // HDFSfilePathMap:输入日志文件类型以及路径(HDFS)配置<br>
    // dbSourceMap:db(mysql) 驱动配置<br>
    // logStructMap：log 日志属性配置<br>
    // tablesDefMap：mysql表定义配置<br>
    val properties = XmlProperiesAnalysis.xmlProperiesAnalysis

    val properiesMap = properties._1
    val logStructMap = properties._4
    val tablesMap = properties._5

    // 各文件流联合成同一个RDD时，用的共通分隔符
    val unionseparator = properiesMap("unionseparator")
    
    //日志文件属性相关获取
    val logPropertiesMap = logStructMap(topicLogType)
    // log数据主key
    val keyItems = logPropertiesMap("rowKey").split(",")
    // 划分rowkey 用
    val logSpace = properiesMap("logSpace")

    //输出为表结构样式　用
    val tbItems = tablesMap("items").split(",")

    // rowkey 连接符（创建主键时，各表字段的连接符）
    val separator = "asiainfoMixSeparator"
      
    inputRDD.map(record => {
      val itemMap = record.toMap
      val keyMap = (for { key <- keyItems } yield (key, itemMap(key))).toMap
      (rowKeyEditor(keyMap, logSpace, separator), record)
    }).groupByKey.map(f => {
      val count = f._2.size
      val sumCost = for { f <- f._2; val map = f.toMap } yield (map("cost").toFloat)

      // 创建db表结构并初始化
      var dbrecord = Map[String, String]()
      // 流计算
      dbrecord += (("bid_success_cnt") -> count.toString)
      dbrecord += (("expose_cnt") -> count.toString)
      dbrecord += (("cost") -> (sumCost.sum).toString)

       // 顺列流字段为db表结构字段[自定义rowkey-更新字段－消息总长度]
      var mesgae = LogTools.setTBSeq(f._1, tbItems, dbrecord, unionseparator)

      //　　第一个字段rowKey ;最后一个字段log_length（为merge结构字段数）
      val merge_size = tbItems.size + 2
      mesgae = mesgae + unionseparator + merge_size.toString
      mesgae
    })
  }

  /**
   * 根据曝光日志和最终要更新的mysql表的主键<br>
   * 编辑曝光日志的rowkey<br>
   * logSpace:设定的log时间分片时长<br>
   * rowkey (表中的顺序): 活动ID+订单ID+素材ID+广告ID+尺寸ID+地域ID+媒体+广告位ID+日志时间<br>
   */
  def rowKeyEditor(keyMap: Map[String, String], logSpace: String, separator: String): String = {
    //编辑曝光日志 主key    
    //日志时间 log_time
    //广告id	ad_id
    //订单id	order_id
    //活动id	activity_id
    //referer	media_url
    //地域id	area_id
    //尺寸id	size_id
    //广告位id	represent_pos
    //素材ID与广告ID相同
    val log_time = LogTools.timeConversion_H(keyMap("log_time")) + LogTools.timeFlg(keyMap("log_time"), logSpace)
    keyMap("activity_id") + separator +
      keyMap("order_id") + separator +
      keyMap("ad_id") + separator +
      keyMap("ad_id") + separator +
      keyMap("size_id") + separator +
      keyMap("area_id") + separator +
      keyMap("media_url") + separator +
      keyMap("represent_pos") + separator +
      log_time
  }
}
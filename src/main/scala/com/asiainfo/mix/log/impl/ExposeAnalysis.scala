package com.asiainfo.mix.log.impl

import com.asiainfo.mix.streaming_log.StreamAction
import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.dstream.DStream
import com.asiainfo.mix.streaming_log.LogTools
import org.apache.spark.Logging
import scala.collection.mutable.ArrayBuffer

/**
 * @author surq
 * @since 2014.07.15
 * 曝光日志 流处理
 */
class ExposeAnalysis extends StreamAction with Serializable {

  /**
   * @param inputStream:log流数据<br>
   * @param xmlParm:解析logconf.xml的结果顺序依次是:<br>
   * 0、kafaArray---[configuration/dataSource/kafakaOut]<br>
   * 1、dbSourceArray----[configuration/dataSource/dbSource]<br>
   * 2、mixLogArray----[configuration/logProperties/log]<br>
   * 3、tablesArray----[configuration/tableDefines/table]<br>
   */
  override def run(inputStream: DStream[Array[(String, String)]], xmlParm: Seq[Array[(String, String)]]): DStream[String] = {
    printInfo(this.getClass(), "ExposeAnalysis is running!")
    val kafaMap = xmlParm(0).toMap
    val logPropertiesMap = xmlParm(2).toMap
    val tablesMap = xmlParm(3).toMap

    // 输出kafka配置 
    val kafkaseparator = kafaMap("separator")
    val brokers = kafaMap("brokers")
    val topic = kafaMap("topic")

    // log数据主key
    val keyItems = logPropertiesMap("rowKey").split(",")
    // 划分rowkey 用
    val logSpace = logPropertiesMap("logSpace")

    //输出为表结构样式　用
    val tbItems = tablesMap("items").split(",")

    // rowkey 连接符
    val separator = "asiainfoMixSeparator"

    inputStream.map(record => {
      val itemMap = record.toMap
      printDebug(this.getClass(), "source DStream:" + itemMap.mkString(","))
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

      // 顺列流字段为db表结构字段
      var mesgae = LogTools.setTBSeq(f._1, tbItems, dbrecord, kafkaseparator)

      //　第一个字段log_length（为merge结构字段数）　第二个字段rowKey
      val merge_size = tbItems.size + 2
      mesgae = mesgae + kafkaseparator + merge_size.toString
      // kafa send
      LogTools.kafkaSend(mesgae, brokers, topic)
      printDebug(this.getClass(), "to kafka topic merge data: " + mesgae)
      f._1
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
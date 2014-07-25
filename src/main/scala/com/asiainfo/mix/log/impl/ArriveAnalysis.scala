package com.asiainfo.mix.log.impl

import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.dstream.DStream
import com.asiainfo.mix.streaming_log.LogTools
import com.asiainfo.mix.streaming_log.StreamAction
import org.apache.spark.Logging
import scala.collection.mutable.ArrayBuffer

/**
 * @author surq
 * @since 2014.07.15
 * 到达日志 流处理
 */
class ArriveAnalysis extends StreamAction with Serializable {

  val ox003: Char = 3
  
  /**
   * @param inputStream:log流数据<br>
   * @param xmlParm:解析logconf.xml的结果顺序依次是:<br>
   * 0、kafaArray---[configuration/dataSource/kafakaOut]<br>
   * 1、dbSourceArray----[configuration/dataSource/dbSource]<br>
   * 2、mixLogArray----[configuration/logProperties/log]<br>
   * 3、tablesArray----[configuration/tableDefines/table]<br>
   */
  override def run(inputStream: DStream[Array[(String, String)]], xmlParm: Seq[Array[(String, String)]]): DStream[String] = {
    printInfo(this.getClass(), "ArriveAnalysis is running!")
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

    inputStream.filter(record => {
      val itemMap = record.toMap
      printDebug(this.getClass(), "source DStream:" + itemMap.mkString(","))
      // 第3个字段时间比第9个字段中时间不大于5分钟的日志条数
      // （time_domain_sizeid_areaid_slotid_cid_oid_adid）
      val idList = LogTools.splitArray(itemMap("id"), ox003.toString, 8)
      val time = if(idList(0).trim=="") 0 else idList(0).toLong
      val logtime = itemMap("log_time").toLong + (5 * 60 * 1000)
      if (logtime <= time) true else false
    }).map(record => {
      // （第4个字段相同并且第9个字段oid相同的多条记录算一条记录）
      val itemMap = record.toMap
      // （time_domain_sizeid_areaid_slotid_cid_oid_adid）

      val idList = LogTools.splitArray(itemMap("id"), ox003.toString, 8)
      val rowkey = itemMap("cookie") + separator + idList(6)
      printDebug(this.getClass(), "map: " + itemMap.mkString(","))
      (rowkey, record)
    }).groupByKey.map(f => {
      val record = (f._2).head
      val itemMap = record.toMap
      val keyMap = (for { key <- keyItems } yield (key, itemMap(key))).toMap
      printDebug(this.getClass(), "groupByKey after map: " + record.mkString(","))
      (rowKeyEditor(keyMap, logSpace, separator), record)
    }).groupByKey.map(f => {
      val count = f._2.size
      // 创建db表结构并初始化
      var dbrecord = Map[String, String]()
      // 流计算
      dbrecord += (("arrive_cnt") -> count.toString)

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
   * 根据到达日志和最终要更新的mysql表的主键<br>
   * 编辑到达日志的rowkey<br>
   * logSpace:设定的log时间分片时长<br>
   * rowkey (表中的顺序): 活动ID+订单ID+素材ID+广告ID+尺寸ID+地域ID+媒体+广告位ID+日志时间<br>
   */
  def rowKeyEditor(keyMap: Map[String, String], logSpace: String, separator: String): String = {
    //到达日志 主key		
    //日志时间	log_time
    //referer	media_url
    //订单id	id	●
    val id = keyMap("id")
    // （time_domain_sizeid_areaid_slotid_cid_oid_adid）
    val idList = LogTools.splitArray(id, ox003.toString, 8)
    // 活动ID
    val activity_id = idList(5)
    //订单ID
    val order_id = idList(6)
    // 素材ID ad_id和material_id相同
    val material_id = idList(7)
    // 广告ID
    val ad_id = idList(7)
    // 尺寸ID
    val size_id = idList(2)
    //地域ID	area_id
    val area_id = idList(3)
    //URL	media_url
    val media_url = idList(1)
    // 广告位ID
    val ad_pos_id = idList(4)
    val log_time = LogTools.timeConversion_H(keyMap("log_time")) + LogTools.timeFlg(keyMap("log_time"), logSpace)
    activity_id + separator +
      order_id + separator +
      material_id + separator +
      ad_id + separator +
      size_id + separator +
      area_id + separator +
      media_url + separator +
      ad_pos_id + separator +
      log_time
  }
}
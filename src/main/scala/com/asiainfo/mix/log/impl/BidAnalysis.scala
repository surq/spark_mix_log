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
 * 竞价日志 流处理
 */
class BidAnalysis extends StreamAction with Serializable {

  /**
   * @param inputStream:log流数据<br>
   * @param xmlParm:解析logconf.xml的结果顺序依次是:<br>
   * 0、kafaArray---[configuration/dataSource/kafakaOut]<br>
   * 1、dbSourceArray----[configuration/dataSource/dbSource]<br>
   * 2、mixLogArray----[configuration/logProperties/log]<br>
   * 3、tablesArray----[configuration/tableDefines/table]<br>
   */
  override def run(inputStream: DStream[Array[(String, String)]], xmlParm: Seq[Array[(String, String)]]):DStream[String] = {
    printInfo(this.getClass(),"BitAnalysis is running!")
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
    // 需要对log进行sum的字段
    //    val logCountItems = logPropertiesMap("logCountItems").split(",")
    // rowkey 连接符
    val separator = "asiainfoMixSeparator"
    inputStream.filter(record => {
      val itemMap = record.toMap
      printDebug(this.getClass(),"source DStream:" + itemMap.mkString(","))
      // 第3个字段【log_type】值为1的日志条数
      // 1.竞价日志里会有两部分内容，a放弃竞价日志以18开头***  b参与竞价20开头****，所需的维度信息均在18列
      if (itemMap("log_type").trim == "1" && itemMap("log_length") == "21") true else false
    }).map(record => {
      val itemMap = record.toMap
      printDebug(this.getClass(),"map:" + itemMap.mkString(","))
      val keyMap = (for { key <- keyItems } yield (key, itemMap(key))).toMap
      (rowKeyEditor(keyMap, logSpace, separator), record)
    }).groupByKey.map(f => {
      val count = f._2.size
      // 创建db表结构并初始化
      var dbrecord = Map[String, String]()
      // 流计算
      dbrecord += (("bid_cnt") -> count.toString)
      // 顺列流字段为db表结构字段
      var mesgae = LogTools.setTBSeq(f._1,tbItems,dbrecord,kafkaseparator)
      
      //　第一个字段log_length（为merge结构字段数）　第二个字段rowKey
      val merge_size =tbItems.size+2
       mesgae = mesgae+ kafkaseparator + merge_size.toString
      
      // kafa send
      LogTools.kafkaSend(mesgae, brokers, topic)
      printDebug(this.getClass(), "to kafka topic merge data: " + mesgae)
      f._1
    })
  }
  
  /**
   * 根据竞价日志和最终要更新的mysql表的主键<br>
   * 编辑竞价日志的rowkey<br>
   * logSpace:设定的log时间分片时长<br>
   * rowkey (表中的顺序): 活动ID+订单ID+素材ID+广告ID+尺寸ID+地域ID+媒体+广告位ID+日志时间<br>
   */
  def rowKeyEditor(keyMap: Map[String, String], logSpace: String, separator: String): String = {
    //竞价日志 主key		
    //请求日期/时间	log_time
    //地域ID	area_id
    //URL	media_url 提取出完整域名
    //参与竞价广告信息	bitted_ad_info 包含多个子字段：(广告ID ad_id、 ad_id和material_id相同、
    //订单ID order_id、活动ID activity_id、广告位ID ad_pos_id、出价 bit_price、sizeid size_id  
	val bitted_ad_infoList = keyMap("bitted_ad_info")
	val ox003: Char = 3
    val bitted_ad_info = (bitted_ad_infoList.split(ox003))(0)
    val ox004: Char = 4
//    //(adId_orderId_cId_slotId_price_sizeId)
    val infoList = LogTools.splitArray(bitted_ad_info, ox004.toString, 6)
//    val infoList = LogTools.splitArray(bitted_ad_info, "%", 6)

    // 活动ID
    val activity_id = infoList(2)
    //订单ID
    val order_id = infoList(1)
    // 素材ID ad_id和material_id相同
    val material_id = infoList(0)
    
    // (slotId_size_locationId_landpage_lowprice)
     val ad_pos_info = keyMap("ad_pos_info")
     val posinfoList = LogTools.splitArray(ad_pos_info, ox004.toString, 5)
    // 广告ID
    val ad_id = posinfoList(2)
    // 尺寸ID
    val size_id = infoList(5)
    //地域ID	area_id
    val area_id = keyMap("area_id")
    //URL	media_url
    val media_url = LogTools.getDomain(keyMap("media_url"))
    // 广告位ID
    val ad_pos_id = infoList(3)
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
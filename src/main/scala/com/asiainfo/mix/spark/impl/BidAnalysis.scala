package com.asiainfo.mix.spark.impl

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext._
import com.asiainfo.mix.streaming_log.LogTools
import org.apache.spark.Logging
import scala.collection.mutable.ArrayBuffer
import com.asiainfo.spark.stream.report.LogReportUtil
import com.asiainfo.mix.spark.BatchStream
import com.asiainfo.mix.spark.XmlProperiesAnalysis
import org.apache.spark.rdd.RDD
import com.asiainfo.mix.spark.MixLog

/**
 * @author surq
 * @since 2014.09.25
 * 竞价日志 batch处理
 */
class BidAnalysis extends  BatchStream  with MixLog with Serializable {

  /**
   * @param topicLogType:日志类型<br>
   * @param inputRDD:数据源<br>
   * 输入的文件结构多了一个头：rowkey，多一个尾:size<br>
   */
  override def run(topicLogType:String, inputRDD: RDD[Array[(String, String)]]): RDD[String] = {
    printInfo(this.getClass(), "BidAnalysis is running!")

    // properiesMap:applacation properies 配置<br>
    // HDFSfilePathMap:输入日志文件类型以及路径(HDFS)配置<br>
    // dbSourceMap:db(mysql) 驱动配置<br>
    // logStructMap：log 日志属性配置<br>
    // tablesDefMap：mysql表定义配置<br>
    val properties = XmlProperiesAnalysis.xmlProperiesAnalysis

    val properiesMap = properties._1
    val logStructMap = properties._4
    val tablesMap = properties._5
    
    //日志文件属性相关获取
    val logPropertiesMap = logStructMap(topicLogType)
    // log数据主key
    val keyItems = logPropertiesMap("rowKey").split(",")
    // 划分rowkey 用
    val logSpace = properiesMap("logSpace")
    //输出为表结构样式　用
    val tbItems = tablesMap("items").split(",")
    // rowkey 连接符
    val separator = "asiainfoMixSeparator"
    // 各文件流联合成同一个RDD时，用的共通分隔符
    val unionseparator = properiesMap("unionseparator")
    
    inputRDD.map(record => {
      val itemMap = record.toMap
      val keyMap = (for { key <- keyItems } yield (key, itemMap(key))).toMap
      (rowKeyEditor(keyMap, logSpace, separator), record)
    }).groupByKey.map(f => {
      val count = f._2.size
      // 创建db表结构并初始化
      var dbrecord = Map[String, String]()
      // 流计算
      dbrecord += (("bid_cnt") -> count.toString)
      // 顺列流字段为db表结构字段[自定义rowkey-更新字段－消息总长度]
      var mesgae = LogTools.setTBSeq(f._1, tbItems, dbrecord, unionseparator)

      //　第一个字段log_length（为merge结构字段数）　第二个字段rowKey
      val merge_size = tbItems.size + 2
      //　　第一个字段rowKey ;最后一个字段log_length（为merge结构字段数）
      mesgae = mesgae + unionseparator + merge_size.toString
      mesgae
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
    // 20(adId_orderId_cId_slotId_price_sizeId)
    val infoList = LogTools.splitArray(bitted_ad_info, ox004.toString, 6)

    //活动ID	●	20(adId_orderId_活动ID_slotId_price_sizeId)
    //订单ID	●	20(adId_订单ID_cId_slotId_price_sizeId)
    //素材ID	●	20(素材ID_orderId_cId_slotId_price_sizeId)
    //广告ID	●	20(广告ID_orderId_cId_slotId_price_sizeId)
    //尺寸ID	●	20(adId_orderId_cId_slotId_price_尺寸ID)

    // 活动ID
    val activity_id = infoList(2)
    //订单ID
    val order_id = infoList(1)
    // 素材ID ad_id和material_id相同
    val material_id = infoList(0)
    // 广告ID
    val ad_id = infoList(0)
    // 尺寸ID
    val size_id = infoList(5)
    // 18(slotId_size_广告位ID_landpage_lowprice)
    val ad_pos_info = keyMap("ad_pos_info")
    val posinfoList = LogTools.splitArray(ad_pos_info, ox004.toString, 5)
    // 广告位ID
    val ad_pos_id = posinfoList(1)
    //地域ID	area_id
    val area_id = keyMap("area_id")
    //URL	media_url
    val media_url = LogTools.getDomain(keyMap("media_url"))
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